#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A professional-grade linter and advisor for Spark Submit commands.
Version: 5.1 "The Scope Fix"
"""

import sys
import re
import shlex
import argparse
import subprocess
import os
import difflib
from dataclasses import dataclass
from enum import Enum, auto

try:
    import colorama
    from colorama import Fore, Style
except ImportError:
    class DummyColor:
        def __getattr__(self, name): return ""
    Fore = Style = DummyColor()

# --- Configuration & Valid Flags ---
VALID_SPARK_FLAGS = {
    '--master', '--deploy-mode', '--class', '--name', '--jars', '--packages',
    '--exclude-packages', '--repositories', '--py-files', '--files', '--conf',
    '--properties-file', '--driver-memory', '--driver-java-options',
    '--driver-library-path', '--driver-class-path', '--executor-memory',
    '--driver-cores', '--supervise', '--queue', '--num-executors',
    '--executor-cores', '--total-executor-cores', '--archives', '--principal',
    '--keytab', '--help', '--verbose', '--version',
}
FLAGS_REQUIRING_VALUES = {
    '--master', '--class', '--name', '--jars', '--packages', '--exclude-packages',
    '--repositories', '--py-files', '--files', '--conf', '--properties-file',
    '--driver-memory', '--driver-cores', '--queue', '--num-executors',
    '--executor-cores', '--total-executor-cores', '--archives', '--principal', '--keytab',
    '--driver-java-options', '--driver-library-path', '--driver-class-path'
}
DEPRECATED_FLAGS = {
    '--driver-class-path': '--jars',
    '--driver-extraJavaOptions': '--driver-java-options'
}
EQUIVALENT_FLAGS = {
    '--num-executors': 'spark.executor.instances',
    '--executor-memory': 'spark.executor.memory',
    '--driver-memory': 'spark.driver.memory',
    '--executor-cores': 'spark.executor.cores',
    '--name': 'spark.app.name'
}
VALID_MASTER_PATTERNS = [
    re.compile(r'^local(\[\d+|\*\])?$'), re.compile(r'^yarn$'),
    re.compile(r'^spark://.+$'), re.compile(r'^k8s://.+$'), re.compile(r'^mesos://.+$')
]
SPARK_PROFILES = {
    "small": {"num-executors": "4", "executor-cores": "2", "executor-memory": "4g", "driver-memory": "2g", "conf": {"spark.sql.shuffle.partitions": "16"}},
    "medium": {"num-executors": "10", "executor-cores": "4", "executor-memory": "8g", "driver-memory": "4g", "conf": {"spark.sql.shuffle.partitions": "80"}},
    "large": {"num-executors": "25", "executor-cores": "4", "executor-memory": "16g", "driver-memory": "8g", "conf": {"spark.sql.shuffle.partitions": "200"}}
}
MAX_EXECUTOR_CORES, MIN_EXECUTOR_MEM_MB, MAX_EXECUTOR_MEM_MB = 5, 2048, 65536
MIN_DRIVER_MEM_MB, MAX_DRIVER_MEM_MB = 1024, 32768
DEFAULT_MEM_OVERHEAD_FACTOR, MIN_MEM_OVERHEAD_MB = 0.10, 384
TERMINAL_FLAGS = {'--help', '--version'}
SPARK_PROPERTY_INTRO_VERSIONS = {
    "spark.sql.adaptive.enabled": "3.0.0",
    "spark.sql.adaptive.coalescePartitions.enabled": "3.0.0",
    "spark.sql.files.maxPartitionBytes": "2.3.0",
    "spark.sql.execution.arrow.pyspark.enabled": "2.3.0",
}

class Status(Enum):
    ERROR, WARNING = auto(), auto()

@dataclass
class ValidationResult:
    status: Status; message: str; suggestion: str = ""

class SparkCommandChecker:
    _cached_detected_version = None

    def __init__(self, command_string):
        self.params, self.app_file, self.app_args, self.results, self.summary_data = {}, None, [], [], {}
        tokens = shlex.split(command_string)
        spark_submit_token_index = -1
        for i, token in enumerate(tokens):
            if re.match(r'^spark\d*-submit$', token):
                spark_submit_token_index = i; break
        if spark_submit_token_index == -1:
            raise ValueError("Command does not appear to contain a valid Spark submit executable.")
        self.original_command_prefix, self.executable, self.args = tokens[:spark_submit_token_index], tokens[spark_submit_token_index], tokens[spark_submit_token_index+1:]

    def _get_param(self, key, conf_key=None):
        direct_flag = f'--{key}'
        if direct_flag in self.params: return self.params[direct_flag]
        if conf_key and self._get_conf(conf_key): return self._get_conf(conf_key)
        return None

    def _get_conf(self, key):
        for conf in self.params.get('--conf', []):
            if conf.startswith(f'{key}='): return conf.split('=', 1)[1]
        return None

    def _parse_memory_to_mb(self, mem_str):
        if not isinstance(mem_str, str): return None
        match = re.match(r'(\d+(?:\.\d+)?)([gGmMkK])', mem_str.strip())
        if not match:
            if '<value>' in mem_str: return 1024 # Return a dummy value for templates
            self.results.append(ValidationResult(Status.ERROR, f"Invalid memory format: '{mem_str}'.", "Use a unit like 'g' or 'm'."))
            return None
        val, unit = float(match.group(1)), match.group(2).lower()
        if unit == 'g': return int(val * 1024)
        if unit == 'm': return int(val)
        return int(val / 1024)

    def _detect_cluster_spark_version(self):
        if SparkCommandChecker._cached_detected_version:
            return SparkCommandChecker._cached_detected_version
        try:
            output = subprocess.check_output(["spark-submit", "--version"], stderr=subprocess.STDOUT, text=True, timeout=5)
            match = re.search(r"version\s+(\d+\.\d+\.\d+)", output, re.IGNORECASE)
            if match:
                SparkCommandChecker._cached_detected_version = match.group(1)
                return SparkCommandChecker._cached_detected_version
        except (subprocess.SubprocessError, FileNotFoundError, TimeoutError):
            pass
        SparkCommandChecker._cached_detected_version = "3.0.0" # Fallback to a modern version
        return SparkCommandChecker._cached_detected_version

    def _parse_command(self):
        processed_args = []
        for arg in self.args:
            if arg.startswith('--') and '=' in arg: processed_args.extend(arg.split('=', 1))
            else: processed_args.append(arg)
        self.params['--conf'] = []
        seen_flags = set()
        i = 0
        while i < len(processed_args):
            arg = processed_args[i]
            if arg.startswith('--'):
                param_name = arg
                if param_name in seen_flags and param_name != '--conf': self.results.append(ValidationResult(Status.ERROR, f"Duplicate parameter found: '{param_name}'."))
                seen_flags.add(param_name)
                if i + 1 >= len(processed_args) or processed_args[i+1].startswith('--'): self.params[param_name] = True; i += 1
                else:
                    if param_name == '--conf': self.params[param_name].append(processed_args[i+1])
                    else: self.params[param_name] = processed_args[i+1]
                    i += 2
            elif self.app_file is None:
                if '.jar' in arg or '.py' in arg:
                    self.app_file, self.app_args = arg, processed_args[i+1:]; break
                else:
                    self.results.append(ValidationResult(Status.ERROR, f"Unexpected positional argument '{arg}' before application file.", "Application arguments must come after the .jar or .py file."))
                    i += 1
            else: i += 1

    def suggest_best_profile(self):
        if not self.params: self._parse_command()
        best_profile_name, min_distance = None, float('inf')
        for name, profile_data in SPARK_PROFILES.items():
            distance = self._calculate_profile_distance(profile_data)
            if distance < min_distance:
                min_distance, best_profile_name = distance, name
        deviations = self._get_profile_deviations(best_profile_name)
        return best_profile_name, deviations

    def _calculate_profile_distance(self, profile):
        total_distance = 0
        weights = {"num-executors": 1.0, "executor-cores": 1.0, "executor-memory": 1.5}
        for key, weight in weights.items():
            profile_val_str = profile.get(key)
            user_val_str = self._get_param(key, f"spark.{key.replace('-', '.')}")
            if profile_val_str and user_val_str:
                try:
                    profile_val = self._parse_memory_to_mb(profile_val_str) if 'memory' in key else int(profile_val_str)
                    user_val = self._parse_memory_to_mb(user_val_str) if 'memory' in key else int(user_val_str)
                    if profile_val and user_val:
                        total_distance += (abs(user_val - profile_val) / profile_val) * weight
                except (ValueError, TypeError): continue
        return total_distance

    def _get_profile_deviations(self, profile_name):
        original_results, self.results = self.results, []
        self._check_profile_conformance(profile_name)
        deviations, self.results = self.results, original_results
        return deviations

    def generate_command_from_profile(self, profile_name):
        if profile_name not in SPARK_PROFILES: return ""
        profile = SPARK_PROFILES[profile_name]
        command_parts = self.original_command_prefix + [self.executable]
        preserved_params = {k: v for k, v in self.params.items() if k not in ['--num-executors', '--executor-cores', '--executor-memory', '--driver-memory', '--conf']}
        for key, val in preserved_params.items(): command_parts.extend([key, str(val)])
        for key, val in profile.items():
            if key != 'conf': command_parts.extend([f'--{key}', val])
        original_confs = {c.split('=', 1)[0]: c.split('=', 1)[1] for c in self.params.get('--conf', []) if '=' in c}
        profile_confs = profile.get('conf', {})
        final_confs = {**original_confs, **profile_confs}
        for key, val in final_confs.items(): command_parts.extend(['--conf', f'{key}={val}'])
        if self.app_file: command_parts.append(self.app_file)
        if self.app_args: command_parts.extend(self.app_args)
        return ' '.join(shlex.quote(part) for part in command_parts)

    def validate(self):
        self._parse_command()
        if any(flag in self.params for flag in TERMINAL_FLAGS):
            flag = next(f for f in TERMINAL_FLAGS if f in self.params)
            self.results.append(ValidationResult(Status.WARNING, f"Terminal flag '{flag}' was used.", "Spark will print information and exit. All other parameters will be ignored."))
            return
        if any(r.status == Status.ERROR for r in self.results): return

        check_registry = [
            self._check_for_missing_values, self._check_conf_format, self._check_required_params,
            self._check_for_typos, self._check_deprecated_flags, self._check_conflicts_and_equivalents,
            self._check_master_url, self._check_logical_inconsistencies,
            self._check_jar_specifics, self._check_duplicate_conf_keys, self._check_bad_conf_practices,
            self._check_deploy_mode_value, self._check_total_cores_logic, self._check_value_formats,
            self._check_mode_specific_logic, self._check_dynamic_allocation_conflicts,
            self._check_resource_math, self._check_obsolete_jvm_options,
            self._check_python_specifics, self._check_cluster_paths,
            self._check_file_existence, self._check_executor_resources, self._check_driver_memory,
            self._check_shuffle_partitions, self._check_yarn_queue, self._check_performance_tuning_advice,
            self._check_executor_mem_per_core, self._check_driver_vs_executor_balance,
            self._check_shuffle_partitions_vs_cores, self._check_mixed_path_schemes,
            self._check_packages_version_specified, self._check_dynamic_allocation_shuffle_service,
            self._check_dynamic_allocation_min_max, self._check_missing_app_name,
            self._check_yarn_principal_without_renewal, self._check_spark_version_compatibility
        ]
        for check_func in check_registry: check_func()

    # --- ALL CHECK METHODS ---
    def _check_for_missing_values(self):
        for flag in FLAGS_REQUIRING_VALUES:
            if flag in self.params and self.params[flag] is True: self.results.append(ValidationResult(Status.ERROR, f"The parameter '{flag}' was provided but is missing its required value."))
    def _check_conf_format(self):
        for conf_arg in self.params.get('--conf', []):
            if '=' not in conf_arg:
                if '<key>' in conf_arg: continue
                suggestion = "To specify a properties file, use --properties-file." if conf_arg.endswith(('.conf', '.properties')) else "The correct format is '--conf key=value'."
                self.results.append(ValidationResult(Status.ERROR, f"Malformed --conf argument: '{conf_arg}'.", suggestion))
    def _check_required_params(self):
        if not self.app_file: self.results.append(ValidationResult(Status.ERROR, "Application file (JAR or .py) is missing."))
        if self.app_file and ('.jar' in self.app_file) and not self._get_param('class'):
            if '<main.class>' not in str(self.params.get('--class')):
                self.results.append(ValidationResult(Status.ERROR, "'--class' is required for .jar applications."))
        if not self._get_param('master'): self.results.append(ValidationResult(Status.ERROR, "'--master' URL is a required parameter."))
    def _check_for_typos(self):
        for param in self.params.keys():
            if param.startswith('--'):
                if param not in VALID_SPARK_FLAGS and param not in DEPRECATED_FLAGS:
                    if param.startswith('--spark.'):
                        self.results.append(ValidationResult(Status.WARNING, f"Unknown parameter '{param}'. Spark properties must be set via --conf.", f"Did you mean to use '--conf {param[2:]}=<value>'?"))
                    else:
                        matches = difflib.get_close_matches(param, VALID_SPARK_FLAGS, n=1, cutoff=0.8)
                        self.results.append(ValidationResult(Status.WARNING, f"Unknown parameter '{param}'.", f"Did you mean '{matches[0]}'?" if matches else ""))
    def _check_deprecated_flags(self):
        for param in self.params:
            if param in DEPRECATED_FLAGS: self.results.append(ValidationResult(Status.WARNING, f"The flag '{param}' is deprecated.", f"Consider using the modern equivalent: '{DEPRECATED_FLAGS[param]}'."))
    def _check_conflicts_and_equivalents(self):
        for flag, conf_key in EQUIVALENT_FLAGS.items():
            if flag in self.params and self._get_conf(conf_key): self.results.append(ValidationResult(Status.WARNING, f"Redundant configuration: Both '{flag}' and '--conf {conf_key}' are set.", "It's best to specify this configuration only once."))
    def _check_master_url(self):
        master_url = self._get_param('master')
        if master_url and '<master-url>' not in master_url and not any(pattern.match(master_url) for pattern in VALID_MASTER_PATTERNS): self.results.append(ValidationResult(Status.WARNING, f"Unconventional master URL: '{master_url}'.", "Ensure this is a valid URL for your cluster (e.g., 'yarn', 'local[*]', 'spark://host:port')."))
    def _check_logical_inconsistencies(self):
        if self.app_file and ('.py' in self.app_file):
            for arg in self.app_args:
                if arg.endswith('.jar'): self.results.append(ValidationResult(Status.WARNING, f"Python application '{self.app_file}' is given a '.jar' file ('{arg}') as an argument.", "Ensure this is not a dependency that should be handled via --jars or --py-files."))
    def _check_file_existence(self):
        if self.app_file and not re.match(r'^[a-zA-Z][a-zA-Z0-9]*:', self.app_file) and not os.path.exists(self.app_file): self.results.append(ValidationResult(Status.WARNING, f"Application file not found at local path: '{self.app_file}'.", "If this path is meant for a remote machine or HDFS, please ensure it exists there."))
    def _check_python_specifics(self):
        if self.app_file and ('.py' in self.app_file):
            if self._get_param('class'): self.results.append(ValidationResult(Status.ERROR, "The '--class' parameter should not be used with Python applications."))
            if self._get_param('jars'): self.results.append(ValidationResult(Status.WARNING, "The '--jars' flag was used with a Python application.", "For Python dependencies, consider using '--py-files'."))
    def _check_jar_specifics(self):
        if self.app_file and ('.jar' in self.app_file):
            if self._get_param('py-files'): self.results.append(ValidationResult(Status.WARNING, "The '--py-files' flag was used with a .jar application.", "This flag is for Python dependencies and will likely have no effect."))

    # --- FIXED METHOD ---
    def _check_duplicate_conf_keys(self):
        conf_keys = [c.split('=', 1)[0] for c in self.params.get('--conf', []) if '=' in c]
        seen = set()
        duplicates = set()
        for k in conf_keys:
            if k in seen:
                duplicates.add(k)
            else:
                seen.add(k)
        if duplicates:
            self.results.append(ValidationResult(Status.WARNING, f"Duplicate configuration key(s) found in --conf: {', '.join(duplicates)}", "The last value set for a key typically takes precedence."))

    def _check_bad_conf_practices(self):
        if self._get_conf('spark.jars'): self.results.append(ValidationResult(Status.WARNING, "Configuration 'spark.jars' was set via --conf.", "Use the dedicated '--jars' flag instead."))
    def _check_deploy_mode_value(self):
        deploy_mode = self._get_param('deploy-mode')
        if deploy_mode and deploy_mode not in ('client', 'cluster', '<deploy-mode>'): self.results.append(ValidationResult(Status.ERROR, f"Invalid value '{deploy_mode}' for --deploy-mode.", "The value must be either 'client' or 'cluster'."))
    def _check_total_cores_logic(self):
        if self._get_param('total-executor-cores') and self._get_param('num-executors') and self._get_param('executor-cores'): self.results.append(ValidationResult(Status.ERROR, "Invalid combination: '--total-executor-cores' cannot be used with both '--num-executors' and '--executor-cores'."))
    def _check_value_formats(self):
        app_name = self._get_param('name')
        if app_name and (app_name.endswith('.jar') or app_name.endswith('.py')): self.results.append(ValidationResult(Status.WARNING, f"The application name '{app_name}' looks like a filename.", "Use a descriptive name, not the filename."))
        for flag in ['--jars', '--files', '--py-files', '--archives']:
            value = self.params.get(flag)
            if value and value.startswith("'") and value.endswith("'"): self.results.append(ValidationResult(Status.WARNING, f"The value for '{flag}' is wrapped in single quotes.", "Spark expects a comma-separated list of paths without outer quotes."))
    def _check_mode_specific_logic(self):
        master, deploy_mode = self._get_param('master'), self._get_param('deploy-mode')
        if master and master.startswith('local') and (self._get_param('num-executors') or self._get_param('executor-cores')): self.results.append(ValidationResult(Status.WARNING, "Resource flags like '--num-executors' are used with local master.", "These flags will be ignored in local mode."))
        if deploy_mode == 'client' and self._get_param('supervise'): self.results.append(ValidationResult(Status.WARNING, "The '--supervise' flag is used with client deploy mode.", "Supervision is only available in cluster mode."))
    def _check_dynamic_allocation_conflicts(self):
        if self._get_conf('spark.dynamicAllocation.enabled') == 'true' and (self._get_param('num-executors') or self._get_param('total-executor-cores')): self.results.append(ValidationResult(Status.WARNING, "Static allocation flags (e.g., --num-executors) are set while dynamic allocation is enabled.", "The static flags will be ignored."))
    def _check_resource_math(self):
        total_cores_str, num_execs_str, cores_per_exec_str = self._get_param('total-executor-cores'), self._get_param('num-executors'), self._get_param('executor-cores')
        if all((total_cores_str, num_execs_str, cores_per_exec_str)):
            try:
                total, num, cores = int(total_cores_str), int(num_execs_str), int(cores_per_exec_str)
                if total != num * cores: self.results.append(ValidationResult(Status.ERROR, f"Resource math is inconsistent: {num} * {cores} != {total}."))
            except (ValueError, TypeError): return
    def _check_obsolete_jvm_options(self):
        opts_to_check = [self._get_param('driver-java-options', 'spark.driver.extraJavaOptions'), self._get_conf('spark.executor.extraJavaOptions')]
        for opts in filter(None, opts_to_check):
            if '-XX:MaxPermSize' in opts: self.results.append(ValidationResult(Status.WARNING, "Obsolete JVM option '-XX:MaxPermSize' was found.", "PermGen was removed in Java 8."))
    def _check_performance_tuning_advice(self):
        master, num_execs_str, cores_per_exec_str = self._get_param('master'), self._get_param('num-executors'), self._get_param('executor-cores')
        if num_execs_str and cores_per_exec_str:
            try:
                num_execs, cores_per_exec = int(num_execs_str), int(cores_per_exec_str)
                if num_execs > 10 and cores_per_exec == 1: self.results.append(ValidationResult(Status.WARNING, "Job configured with many executors but only 1 core each.", "Consider decreasing executor count and increasing cores per executor."))
                if master and master in ('yarn', 'k8s', 'mesos') and 0 < num_execs < 3: self.results.append(ValidationResult(Status.WARNING, f"A small job ({num_execs} executors) is being submitted to a cluster.", "Consider using local mode for efficiency."))
            except (ValueError, TypeError): return
    def _check_executor_resources(self):
        cores_str = self._get_param('executor-cores', 'spark.executor.cores')
        if cores_str and cores_str.isdigit() and int(cores_str) > MAX_EXECUTOR_CORES: self.results.append(ValidationResult(Status.WARNING, f"High executor cores ({cores_str}).", "Suggestion: Decrease cores to 5 or less and increase '--num-executors'."))
    def _check_driver_memory(self):
        driver_mem_str = self._get_param('driver-memory', 'spark.driver.memory')
        if not driver_mem_str: return
        driver_mem_mb = self._parse_memory_to_mb(driver_mem_str)
        if driver_mem_mb:
            if driver_mem_mb < MIN_DRIVER_MEM_MB: self.results.append(ValidationResult(Status.WARNING, f"Low driver memory ({driver_mem_str})."))
            if driver_mem_mb > MAX_DRIVER_MEM_MB: self.results.append(ValidationResult(Status.WARNING, f"High driver memory specified ({driver_mem_str}).", "Ensure the driver node has sufficient physical memory."))
    def _check_executor_mem_per_core(self):
        cores_str, mem_str = self._get_param('executor-cores'), self._get_param('executor-memory')
        if cores_str and mem_str:
            try:
                cores, mem_mb = int(cores_str), self._parse_memory_to_mb(mem_str)
                if cores > 0 and mem_mb and (mem_mb / 1024 / cores) < 1: self.results.append(ValidationResult(Status.WARNING, "Low executor memory per core.", "At least 1 GB per core is typical."))
            except (ValueError, TypeError): pass
    def _check_driver_vs_executor_balance(self):
        driver_mem, exec_mem = self._parse_memory_to_mb(self._get_param('driver-memory')), self._parse_memory_to_mb(self._get_param('executor-memory'))
        if driver_mem and exec_mem and driver_mem < exec_mem / 2: self.results.append(ValidationResult(Status.WARNING, "Driver memory is much smaller than executor memory.", "Consider increasing driver memory for heavy collect() operations."))
    def _check_shuffle_partitions(self):
        if not self._get_conf('spark.sql.shuffle.partitions'):
            self.results.append(ValidationResult(Status.WARNING, "'spark.sql.shuffle.partitions' is not set.", "Suggestion: Set via --conf. A good starting point is 1.5x to 2x the total number of executor cores."))
    def _check_shuffle_partitions_vs_cores(self):
        shuffle_parts_str, num_execs_str, cores_str = self._get_conf('spark.sql.shuffle.partitions'), self._get_param('num-executors'), self._get_param('executor-cores')
        if all((shuffle_parts_str, num_execs_str, cores_str)):
            try:
                shuffle_parts, num_execs, cores = int(shuffle_parts_str), int(num_execs_str), int(cores_str)
                ideal_min = num_execs * cores
                if shuffle_parts < ideal_min or shuffle_parts > ideal_min * 3: self.results.append(ValidationResult(Status.WARNING, f"'spark.sql.shuffle.partitions' is outside the typical range of {ideal_min}-{ideal_min*3}."))
            except (ValueError, TypeError): pass
    def _check_mixed_path_schemes(self):
        schemes = {p.split('://', 1)[0] if '://' in p else 'local' for flag in ['--files', '--jars', '--py-files', '--archives'] if self.params.get(flag) for p in self.params[flag].split(',')}
        if len(schemes) > 1: self.results.append(ValidationResult(Status.WARNING, f"Mixed dependency path schemes detected: {', '.join(sorted(schemes))}.", "Use consistent schemes to avoid inconsistencies."))
    def _check_packages_version_specified(self):
        packages = self.params.get('--packages')
        if packages and any(p.count(':') == 1 for p in packages.split(',')): self.results.append(ValidationResult(Status.WARNING, "A package may be missing its version.", "Format should be group:artifact:version."))
    def _check_dynamic_allocation_shuffle_service(self):
        if self._get_conf('spark.dynamicAllocation.enabled') == 'true' and self._get_conf('spark.shuffle.service.enabled') != 'true': self.results.append(ValidationResult(Status.WARNING, "Dynamic allocation is enabled but shuffle service is not.", "Enable spark.shuffle.service.enabled to prevent data loss on executor removal."))
    def _check_dynamic_allocation_min_max(self):
        min_exec, max_exec = self._get_conf('spark.dynamicAllocation.minExecutors'), self._get_conf('spark.dynamicAllocation.maxExecutors')
        if min_exec and max_exec:
            try:
                if int(min_exec) > int(max_exec): self.results.append(ValidationResult(Status.ERROR, f"Dynamic allocation minExecutors ({min_exec}) > maxExecutors ({max_exec})."))
            except ValueError: pass
    def _check_missing_app_name(self):
        if not self._get_param('name', 'spark.app.name'): self.results.append(ValidationResult(Status.WARNING, "Application name is not set.", "Use --name to make tracking in Spark UI easier."))
    def _check_yarn_principal_without_renewal(self):
        if self._get_param('principal') and self._get_param('keytab') and not self._get_conf('spark.yarn.kerberos.renewal.time'): self.results.append(ValidationResult(Status.WARNING, "Kerberos is used but no renewal time is configured.", "Set spark.yarn.kerberos.renewal.time for long-running jobs."))
    def _check_spark_version_compatibility(self):
        cluster_version = self._get_conf('spark.version') or self._detect_cluster_spark_version()
        def v_tuple(v): return tuple(int(p) for p in re.findall(r'\d+', v)[:3])
        cluster_vt = v_tuple(cluster_version)
        for key in [c.split('=',1)[0] for c in self.params.get('--conf',[])]:
            if key in SPARK_PROPERTY_INTRO_VERSIONS and v_tuple(SPARK_PROPERTY_INTRO_VERSIONS[key]) > cluster_vt:
                self.results.append(ValidationResult(Status.WARNING, f"Config '{key}' requires Spark >= {SPARK_PROPERTY_INTRO_VERSIONS[key]}, but detected version is {cluster_version}."))
    def _check_cluster_paths(self):
        if self._get_param('deploy-mode') != 'cluster': return
        for flag in ['--files', '--py-files', '--archives']:
            paths_str = self.params.get(flag)
            if paths_str:
                for path in paths_str.split(','):
                    if path.strip().startswith('/') and not any(path.strip().startswith(p) for p in ['hdfs:', 's3a:', 'gs:', 'abfs:']):
                        self.results.append(ValidationResult(Status.WARNING, f"Local path '{path.strip()}' used in '{flag}' with cluster deploy mode.", "This path may not be accessible on cluster nodes. Use a distributed filesystem."))
    def _check_yarn_queue(self):
        if 'yarn' in str(self._get_param('master')) and not self._get_param('queue'): self.results.append(ValidationResult(Status.WARNING, "YARN queue is not specified.", "Suggestion: Use '--queue' in multi-tenant clusters."))
    def _check_profile_conformance(self, profile_name):
        profile_name = profile_name.lower()
        if profile_name not in SPARK_PROFILES:
            self.results.append(ValidationResult(Status.WARNING, f"Profile '{profile_name}' not found.", f"Available profiles: {', '.join(SPARK_PROFILES.keys())}")); return
        profile = SPARK_PROFILES[profile_name]
        for key, expected_value in profile.items():
            if key == 'conf':
                for conf_key, conf_val in expected_value.items():
                    actual_conf_val = self._get_conf(conf_key)
                    if not actual_conf_val: self.results.append(ValidationResult(Status.WARNING, f"Profile '{profile_name}' expects conf '{conf_key}', but it was not set."))
                    elif actual_conf_val != conf_val: self.results.append(ValidationResult(Status.WARNING, f"Conf '{conf_key}' value '{actual_conf_val}' does not match profile '{profile_name}'.", f"Suggestion: Use the standard profile value of '{conf_val}'."))
            else:
                param_value = self._get_param(key)
                if not param_value: self.results.append(ValidationResult(Status.WARNING, f"Profile '{profile_name}' expects '--{key}', but it was not set."))
                elif str(param_value) != str(expected_value): self.results.append(ValidationResult(Status.WARNING, f"Parameter '--{key}' value '{param_value}' does not match profile '{profile_name}'.", f"Suggestion: Use the standard profile value of '{expected_value}'."))

    def print_report(self, no_color=False, general_only=False):
        if no_color: colorama.deinit()
        results_to_print = [r for r in self.results if "profile" not in r.message.lower()] if general_only else self.results
        errors, warnings = [r for r in results_to_print if r.status == Status.ERROR], [r for r in results_to_print if r.status == Status.WARNING]
        
        if not general_only: print(f"{Style.BRIGHT}--- Spark Submit Command Analysis ---{Style.RESET_ALL}")
        if not results_to_print:
             print(f"\n{Fore.GREEN}✅ All relevant checks passed!{Style.RESET_ALL}")
             return

        if errors:
            print(f"\n{Fore.RED}❌ {len(errors)} ERROR(S) FOUND:{Style.RESET_ALL}")
            for i, res in enumerate(errors, 1):
                print(f"  {i}. {res.message}")
                if res.suggestion: print(f"     {Fore.CYAN}↳ {res.suggestion}{Style.RESET_ALL}")
        if warnings:
            print(f"\n{Fore.YELLOW}⚠️ {len(warnings)} WARNING(S) FOUND:{Style.RESET_ALL}")
            for i, res in enumerate(warnings, 1):
                print(f"  {i}. {res.message}")
                if res.suggestion: print(f"     {Fore.CYAN}↳ {res.suggestion}{Style.RESET_ALL}")
        
        if not general_only:
            print(f"\n{Style.BRIGHT}📋--- Configuration Summary ---{Style.RESET_ALL}")
            summary_items = {'Executable': self.executable, 'Application': self.app_file, 'Master': self._get_param('master'), 'Deploy Mode': self._get_param('deploy-mode', 'client (default)')}
            for key, val in summary_items.items():
                if val is not None: print(f"  - {key:<30}: {val}")
        print("\n--- End of Report ---\n")

def main():
    colorama.init(autoreset=True)
    parser = argparse.ArgumentParser(description="A professional linter and advisor for Spark Submit commands.", formatter_class=argparse.RawTextHelpFormatter, epilog="Example:\n  python %(prog)s \"spark-submit ...\"")
    parser.add_argument("command_string", nargs="?", help="The full spark-submit command string to check, enclosed in quotes.")
    parser.add_argument("-p", "--profile", help="[Strict Mode] Check conformance against a specific job profile and exit.")
    parser.add_argument("--no-color", action="store_true", help="Disable colored output.")
    args = parser.parse_args()

    if not args.command_string:
        parser.print_help(); sys.exit(0)

    checker = SparkCommandChecker(args.command_string)
    if args.profile:
        checker.validate()
        checker.print_report(no_color=args.no_color)
    else: # Interactive Advisor Mode
        print(f"{Style.BRIGHT}--- Spark Submit Interactive Advisor ---{Style.RESET_ALL}")
        checker.validate()
        if any(r.status == Status.ERROR for r in checker.results):
            checker.print_report(no_color=args.no_color); sys.exit(1)
        best_profile, deviations = checker.suggest_best_profile()
        print(f"\n{Fore.GREEN}✅ Based on your parameters, the closest standard profile is '{best_profile}'.{Style.RESET_ALL}")
        if not deviations:
            print(f"{Fore.GREEN}Your command already conforms to this profile! Reviewing other best practices...{Style.RESET_ALL}")
        else:
            print(f"\n{Fore.YELLOW}⚠️ The following changes are recommended to conform to the '{best_profile}' profile standard:{Style.RESET_ALL}")
            for res in deviations:
                message = res.message.replace(f"Profile '{best_profile}' expects ", "")
                print(f"  - {message}")
                if res.suggestion: print(f"    {Fore.CYAN}↳ {res.suggestion}{Style.RESET_ALL}")
            try:
                choice = input(f"\n{Style.BRIGHT}Apply these changes and output the corrected command? (y/n): {Style.RESET_ALL}").lower()
                if choice == 'y':
                    print(f"\n{Style.BRIGHT}📋--- Corrected Command for '{best_profile}' Profile ---{Style.RESET_ALL}")
                    suggested_command = checker.generate_command_from_profile(best_profile)
                    print(f"\n{Fore.MAGENTA}{suggested_command}{Style.RESET_ALL}\n")
                    sys.exit(0)
            except KeyboardInterrupt: print("\n\nOperation cancelled. Exiting."); sys.exit(1)
        print("\n" + "="*50 + "\n" + f"{Style.BRIGHT}--- General Linting & Best Practices Report ---{Style.RESET_ALL}")
        checker.print_report(no_color=args.no_color, general_only=True)

if __name__ == "__main__":
    main()