import os
import re
import subprocess
from textwrap import dedent

import pytest


def python_executable():
    return os.path.abspath(os.path.dirname(__file__) + "/../.venv/bin/python")


@pytest.fixture
def tasks_repo_path(tmp_path):
    path = tmp_path / "tasks"
    path.mkdir()
    return path


@pytest.fixture
def output_dir(tmp_path):
    path = tmp_path / "output"
    path.mkdir()
    return path


def strip_dates(string):
    return re.sub(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+", "2023-11-23 21:36:52.983", string)


@pytest.fixture
def job_manifest(tasks_repo_path, print_env_task, filter_env_task, output_dir, tmp_path):
    job = dedent(
        f"""
        name: test-job
        description: A test job to run end-to-end tests on
        data: {output_dir}
        tasks: {tasks_repo_path}
        env:
          JOB_VAR: job-var-value
        commands:
          - name: print-env
            task: print-env
            env:
              INPUT1: 100
              INPUT2: false
              TEMP_FILE: ${{tmp.file}}
              OUTPUT: $DATA/env.txt
          - name: filter-env
            task: filter
            env:
              FILE: ${{previous.OUTPUT}}
              PATTERN: -i input
              OUTPUT: $DATA/result.txt
        """
    )
    job_dir = tmp_path / "test-job"
    job_dir.mkdir()
    job_path = job_dir / "job.yml"
    job_path.write_text(job, encoding="utf-8")
    return job_path


@pytest.fixture
def print_env_task(tasks_repo_path):
    task_dir = tasks_repo_path / "print-env"
    task_dir.mkdir(parents=True, exist_ok=True)

    print_env_task = dedent(
        """
        name: print-env
        description: Prints all env variables
        env:
          OUTPUT:
            description: File to write env values to
            type: string
          TEMP_FILE:
            description: File to write temp values to
            type: string
          INPUT1:
            description: First input variable
            type: int
          INPUT2:
            description: Second input variable
            type: bool
        run: ./print_env.sh
        """
    )
    print_env_task_path = task_dir / "manifest.yml"
    print_env_task_path.write_text(print_env_task, encoding="utf-8")

    print_env_script = dedent(
        """
        #!/bin/bash
        echo "Temp values stored at $TEMP_FILE"
        /usr/bin/env > $TEMP_FILE
        ls "$TEMP_FILE"
        cat $TEMP_FILE > $OUTPUT
        """
    ).strip()
    print_env_script_path = task_dir / "print_env.sh"
    print_env_script_path.write_text(print_env_script, encoding="utf-8")
    print_env_script_path.chmod(0o755)

    return print_env_task_path


@pytest.fixture
def filter_env_task(tasks_repo_path):
    task_dir = tasks_repo_path / "filter"
    task_dir.mkdir(parents=True, exist_ok=True)

    filter_env_task = dedent(
        """
        name: filter
        description: Concatenate files listed in an input file
        env:
          FILE:
            descriptiong: File to filter lines from
            type: string
          PATTERN:
            description: Pattern to filter lines with
            type: string
          OUTPUT:
            description: File to write concatenated files to
            type: string
        run:
          interpreter: /bin/bash -c
          script: cat $FILE | grep $PATTERN | tee $OUTPUT
        """
    )
    filter_env_task_path = task_dir / "manifest.yml"
    (filter_env_task_path).write_text(filter_env_task, encoding="utf-8")

    return filter_env_task_path


@pytest.fixture
def minimal_job_manifest(tasks_repo_path, output_dir, tmp_path):
    task_dir = tasks_repo_path / "echo"
    task_dir.mkdir(parents=True, exist_ok=True)

    filter_env_task = dedent(
        """
        name: echo
        env:
          MESSAGE:
            descriptiong: The message to print
            type: string
        run:
          interpreter: /bin/bash -c
          script: echo $MESSAGE
        """
    )
    filter_env_task_path = task_dir / "manifest.yml"
    (filter_env_task_path).write_text(filter_env_task, encoding="utf-8")

    job = dedent(
        f"""
        name: minimal-test-job
        description: A test job to run end-to-end tests on
        data: {output_dir}
        tasks: {tasks_repo_path}
        commands:
          - name: echo
            task: echo
            env:
              MESSAGE: Hello world!
        """
    )
    job_path = tmp_path / "job.yml"
    (job_path).write_text(job, encoding="utf-8")
    return job_path


def test_execute_bash_job(job_manifest, output_dir, tmp_path):
    result = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "xetl",
            str(job_manifest),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    # Print the output if it wasn't successful
    assert result.returncode == 0, result.stdout.decode("utf-8")

    # Test resulting files
    assert os.path.exists(str(output_dir / "env.txt")), "The first command's file should have been created"
    assert os.path.exists(str(output_dir / "result.txt")), "The final result file should have been created"
    with open(str(output_dir / "result.txt"), "r") as fd:
        assert fd.readlines() == ["INPUT1=100\n", "INPUT2=False\n"]

    # Test output
    if tmp_path_match := re.search(r"[^ ]*output/tmp/\w*", result.stdout.decode("utf-8")):
        tmp_file = tmp_path_match.group(0)
    else:
        tmp_file = "/tmp"
    expected_output = dedent(
        """
        Loading job manifest at: {job_path}/test-job/job.yml
        ╭──╴Executing job: test-job ╶╴╴╶ ╶
        │ Parsed manifest for job: test-job
        │ Discovering tasks at paths: ['{job_path}/tasks']
        │ Loading task at: {job_path}/tasks/print-env/manifest.yml
        │ Loading task at: {job_path}/tasks/filter/manifest.yml
        │ Available tasks detected:
        │  - print-env
        │  - filter
        ┏━━╸Executing command: print-env (1 of 2) ━╴╴╶ ╶
        ┃   name: print-env
        ┃   description: null
        ┃   task: print-env
        ┃   env:
        ┃     JOB_VAR: job-var-value
        ┃     INPUT1: 100
        ┃     INPUT2: false
        ┃     TEMP_FILE: {tmp_file}
        ┃     OUTPUT: {job_path}/output/env.txt
        ┃   skip: false
        ┃╭──╴Executing task: print-env ─╴╴╶ ╶
        ┃│2023-11-23 21:36:52.983┊ WARNING Ignoring unexpected env variable for task `print-env`: JOB_VAR. Valid names are: OUTPUT, TEMP_FILE, INPUT1, INPUT2
        ┃│2023-11-23 21:36:52.983┊ Temp values stored at {tmp_file}
        ┃│2023-11-23 21:36:52.983┊ {tmp_file}
        ┃╰──╴Return code: 0 ─╴╴╶ ╶
        ┃{space}
        ┏━━╸Executing command: filter-env (2 of 2) ━╴╴╶ ╶
        ┃   name: filter-env
        ┃   description: null
        ┃   task: filter
        ┃   env:
        ┃     JOB_VAR: job-var-value
        ┃     FILE: {job_path}/output/env.txt
        ┃     PATTERN: -i input
        ┃     OUTPUT: {job_path}/output/result.txt
        ┃   skip: false
        ┃╭──╴Executing task: filter ─╴╴╶ ╶
        ┃│2023-11-23 21:36:52.983┊ WARNING Ignoring unexpected env variable for task `filter`: JOB_VAR. Valid names are: FILE, PATTERN, OUTPUT
        ┃│2023-11-23 21:36:52.983┊ INPUT1=100
        ┃│2023-11-23 21:36:52.983┊ INPUT2=False
        ┃╰──╴Return code: 0 ─╴╴╶ ╶
        │ Done! \\o/
        """
    ).format(job_path=str(tmp_path), space=" ", tmp_file=tmp_file)
    actual_result = result.stdout.decode("utf-8")
    assert strip_dates(actual_result.strip()) == strip_dates(expected_output.strip())


def test_execute_bash_job_dryrun(job_manifest, tmp_path):
    result = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "xetl",
            job_manifest,
            "--dryrun",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    # Print the output if it wasn't successful
    assert result.returncode == 0, result.stdout.decode("utf-8")

    if tmp_path_match := re.search(r"[^ ]*output/tmp/\w*", result.stdout.decode("utf-8")):
        tmp_file = tmp_path_match.group(0)
    else:
        tmp_file = "/tmp"
    expected_output = dedent(
        """
        Loading job manifest at: {job_path}/test-job/job.yml
        ╭──╴Executing job: test-job ╶╴╴╶ ╶
        │ Manifest parsed as:
        │   name: test-job
        │   description: A test job to run end-to-end tests on
        │   path: {job_path}/test-job
        │   data: {job_path}/output
        │   env:
        │     JOB_VAR: job-var-value
        │   tasks:
        │   - {job_path}/tasks
        │   commands:
        │   - name: print-env
        │     task: print-env
        │     env:
        │       JOB_VAR: job-var-value
        │       INPUT1: 100
        │       INPUT2: false
        │       TEMP_FILE: {tmp_file}
        │       OUTPUT: {job_path}/output/env.txt
        │   - name: filter-env
        │     task: filter
        │     env:
        │       JOB_VAR: job-var-value
        │       FILE: {job_path}/output/env.txt
        │       PATTERN: -i input
        │       OUTPUT: {job_path}/output/result.txt
        │ Discovering tasks at paths: ['{job_path}/tasks']
        │ Loading task at: {job_path}/tasks/print-env/manifest.yml
        │ Loading task at: {job_path}/tasks/filter/manifest.yml
        │ Available tasks detected:
        │  - print-env
        │  - filter
        ┏━━╸Executing command: print-env (1 of 2) ━╴╴╶ ╶
        ┃   name: print-env
        ┃   description: null
        ┃   task: print-env
        ┃   env:
        ┃     JOB_VAR: job-var-value
        ┃     INPUT1: 100
        ┃     INPUT2: false
        ┃     TEMP_FILE: {tmp_file}
        ┃     OUTPUT: {job_path}/output/env.txt
        ┃   skip: false
        ┃╭──╴Executing task: print-env ─╴╴╶ ╶
        ┃│2023-11-23 21:36:52.983┊ WARNING Ignoring unexpected env variable for task `print-env`: JOB_VAR. Valid names are: OUTPUT, TEMP_FILE, INPUT1, INPUT2
        ┃│2023-12-12 21:46:35.601┊ DRYRUN: Would execute with:
        ┃│2023-12-12 21:46:35.601┊   run: ./print_env.sh
        ┃│2023-12-12 21:46:35.601┊   cwd: {job_path}/tasks/print-env
        ┃│2023-12-12 21:46:35.601┊   env: JOB_VAR=job-var-value, INPUT1=100, INPUT2=False, TEMP_FILE={tmp_file}, OUTPUT={job_path}/output/env.txt
        ┃╰──╴Return code: 0 ─╴╴╶ ╶
        ┃{space}
        ┏━━╸Executing command: filter-env (2 of 2) ━╴╴╶ ╶
        ┃   name: filter-env
        ┃   description: null
        ┃   task: filter
        ┃   env:
        ┃     JOB_VAR: job-var-value
        ┃     FILE: {job_path}/output/env.txt
        ┃     PATTERN: -i input
        ┃     OUTPUT: {job_path}/output/result.txt
        ┃   skip: false
        ┃╭──╴Executing task: filter ─╴╴╶ ╶
        ┃│2023-11-23 21:36:52.983┊ WARNING Ignoring unexpected env variable for task `filter`: JOB_VAR. Valid names are: FILE, PATTERN, OUTPUT
        ┃│2023-12-12 21:46:35.602┊ DRYRUN: Would execute with:
        ┃│2023-11-23 21:36:52.983┊   run: /bin/bash -c cat $FILE | grep $PATTERN | tee $OUTPUT
        ┃│2023-12-12 21:46:35.603┊   cwd: {job_path}/tasks/filter
        ┃│2023-12-12 21:46:35.603┊   env: JOB_VAR=job-var-value, FILE={job_path}/output/env.txt, PATTERN=-i input, OUTPUT={job_path}/output/result.txt
        ┃╰──╴Return code: 0 ─╴╴╶ ╶
        │ Done! \\o/
        """
    ).format(job_path=str(tmp_path), space=" ", tmp_file=tmp_file)
    actual_result = result.stdout.decode("utf-8")
    assert strip_dates(actual_result.strip()) == strip_dates(expected_output.strip())


def test_execute_with_minimal_logging_no_timestamps(minimal_job_manifest, tmp_path):
    result = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "xetl",
            minimal_job_manifest,
            "--log-style",
            "minimal",
            "--no-timestamps",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    expected_output = dedent(
        """
        Loading job manifest at: {data_dir}/job.yml
        Executing job: minimal-test-job
        Parsed manifest for job: minimal-test-job
        Discovering tasks at paths: ['{data_dir}/tasks']
        Loading task at: {data_dir}/tasks/echo/manifest.yml
        Available tasks detected:
         - echo
        Executing command: echo (1 of 1)
          name: echo
          description: null
          task: echo
          env:
            MESSAGE: Hello world!
          skip: false
        Executing task: echo
        Hello world!
        Return code: 0
        Done! \o/
        """
    ).format(data_dir=str(tmp_path), space=" ", error_code=1)
    actual_result = result.stdout.decode("utf-8")
    assert strip_dates(actual_result.strip()) == strip_dates(expected_output.strip())


def test_execute_with_moderate_logging_no_timestamps(minimal_job_manifest, tmpdir):
    result = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "xetl",
            minimal_job_manifest,
            "--log-style",
            "moderate",
            "--no-timestamps",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    expected_output = dedent(
        """
        Loading job manifest at: {data_dir}/job.yml
        ─╴Executing job: minimal-test-job╶─
        Parsed manifest for job: minimal-test-job
        Discovering tasks at paths: ['{data_dir}/tasks']
        Loading task at: {data_dir}/tasks/echo/manifest.yml
        Available tasks detected:
         - echo
        ━╸Executing command: echo (1 of 1)╺━
          name: echo
          description: null
          task: echo
          env:
            MESSAGE: Hello world!
          skip: false
        ═╴Executing task: echo╶═
        Hello world!
        ═╴Return code: 0╶═
        Done! \o/
        """
    ).format(data_dir=str(tmpdir), space=" ", error_code=1)
    actual_result = result.stdout.decode("utf-8")
    assert strip_dates(actual_result.strip()) == strip_dates(expected_output.strip())


def test_nested_job(minimal_job_manifest, tasks_repo_path, tmpdir):
    inner_job_path = minimal_job_manifest
    outer_job = dedent(
        f"""
        name: outer-job
        description: The outer job with a command to trigger another nested job
        data: {tmpdir}
        tasks: {tasks_repo_path}
        env:
            JOB_VAR: job-var-value
        commands:
            - name: inner-job
              task: inner-job
        """
    )
    outer_job_path = tmpdir / "outer_job.yml"
    (outer_job_path).write_text(outer_job, encoding="utf-8")

    inner_job_task = dedent(
        f"""
        name: inner-job
        description: This is a task that executes another job
        run: {python_executable()} -m xetl {inner_job_path} --no-timestamps
        """
    )
    task_path = tasks_repo_path / "inner-job"
    task_path.mkdir()
    filter_env_task_path = task_path / "manifest.yml"
    (filter_env_task_path).write_text(inner_job_task, encoding="utf-8")

    result = subprocess.run(
        [
            python_executable(),
            "-m",
            "xetl",
            outer_job_path,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    expected_output = dedent(
        """
        Loading job manifest at: {data_dir}/outer_job.yml
        ╭──╴Executing job: outer-job ╶╴╴╶ ╶
        │ Parsed manifest for job: outer-job
        │ Discovering tasks at paths: ['{data_dir}/tasks']
        │ Loading task at: {data_dir}/tasks/inner-job/manifest.yml
        │ Loading task at: {data_dir}/tasks/echo/manifest.yml
        │ Available tasks detected:
        │  - inner-job
        │  - echo
        ┏━━╸Executing command: inner-job (1 of 1) ━╴╴╶ ╶
        ┃   name: inner-job
        ┃   description: null
        ┃   task: inner-job
        ┃   env:
        ┃     JOB_VAR: job-var-value
        ┃   skip: false
        ┃╭──╴Executing task: inner-job ─╴╴╶ ╶
        ┃│2023-11-23 21:36:52.983┊ WARNING Ignoring unexpected env variable for task `inner-job`: JOB_VAR.
        ┃│2023-11-23 21:36:52.983┊ Loading job manifest at: {data_dir}/job.yml
        ┃│2023-11-23 21:36:52.983┊ ╭──╴Executing job: minimal-test-job ╶╴╴╶ ╶
        ┃│2023-11-23 21:36:52.983┊ │ Parsed manifest for job: minimal-test-job
        ┃│2023-11-23 21:36:52.983┊ │ Discovering tasks at paths: ['{data_dir}/tasks']
        ┃│2023-11-23 21:36:52.983┊ │ Loading task at: {data_dir}/tasks/inner-job/manifest.yml
        ┃│2023-11-23 21:36:52.983┊ │ Loading task at: {data_dir}/tasks/echo/manifest.yml
        ┃│2023-11-23 21:36:52.983┊ │ Available tasks detected:
        ┃│2023-11-23 21:36:52.983┊ │  - inner-job
        ┃│2023-11-23 21:36:52.983┊ │  - echo
        ┃│2023-11-23 21:36:52.983┊ ┏━━╸Executing command: echo (1 of 1) ━╴╴╶ ╶
        ┃│2023-11-23 21:36:52.983┊ ┃   name: echo
        ┃│2023-11-23 21:36:52.983┊ ┃   description: null
        ┃│2023-11-23 21:36:52.983┊ ┃   task: echo
        ┃│2023-11-23 21:36:52.983┊ ┃   env:
        ┃│2023-11-23 21:36:52.983┊ ┃     MESSAGE: Hello world!
        ┃│2023-11-23 21:36:52.983┊ ┃   skip: false
        ┃│2023-11-23 21:36:52.983┊ ┃╭──╴Executing task: echo ─╴╴╶ ╶
        ┃│2023-11-23 21:36:52.983┊ ┃│ Hello world!
        ┃│2023-11-23 21:36:52.983┊ ┃╰──╴Return code: 0 ─╴╴╶ ╶
        ┃│2023-11-23 21:36:52.983┊ │ Done! \o/
        ┃╰──╴Return code: 0 ─╴╴╶ ╶
        │ Done! \o/
        """
    ).format(data_dir=str(tmpdir), space=" ")
    actual_result = result.stdout.decode("utf-8")
    assert strip_dates(actual_result.strip()) == strip_dates(expected_output.strip())


def test_execute_with_failure(output_dir, tasks_repo_path, tmpdir):
    job = dedent(
        f"""
        name: test-job
        description: A test job to run end-to-end tests on
        data: {output_dir}
        tasks: {tasks_repo_path}
        commands:
          - name: fail
            task: fail
        """
    )
    job_path = tmpdir / "job.yml"
    (job_path).write_text(job, encoding="utf-8")

    filter_env_task = dedent(
        """
        name: fail
        description: This is a task that always fails
        run: cat /file/that/doesnt/exist
        """
    )
    task_path = tasks_repo_path / "filter"
    task_path.mkdir()
    filter_env_task_path = task_path / "manifest.yml"
    (filter_env_task_path).write_text(filter_env_task, encoding="utf-8")

    result = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "xetl",
            job_path,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    expected_return_code = 1
    assert result.returncode == expected_return_code, result.stdout.decode("utf-8")

    expected_output = dedent(
        """
        Loading job manifest at: {data_dir}/job.yml
        ╭──╴Executing job: test-job ╶╴╴╶ ╶
        │ Parsed manifest for job: test-job
        │ Discovering tasks at paths: ['{data_dir}/tasks']
        │ Loading task at: {data_dir}/tasks/filter/manifest.yml
        │ Available tasks detected:
        │  - fail
        ┏━━╸Executing command: fail (1 of 1) ━╴╴╶ ╶
        ┃   name: fail
        ┃   description: null
        ┃   task: fail
        ┃   env: {{}}
        ┃   skip: false
        ┃╭──╴Executing task: fail ─╴╴╶ ╶
        ┃│2023-11-23 21:36:52.983┊ cat: /file/that/doesnt/exist: No such file or directory
        ┃╰──╴Return code: {error_code} ─╴╴╶ ╶
        Task failed, terminating job.
        """
    ).format(data_dir=str(tmpdir), space=" ", error_code=expected_return_code)
    actual_result = result.stdout.decode("utf-8")
    assert strip_dates(actual_result.strip()) == strip_dates(expected_output.strip())


def test_invalid_job_yaml(tmp_path):
    result = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "xetl",
            str(tmp_path / "job.yml"),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    actual_output = result.stdout.decode("utf-8")
    assert result.returncode == 1, actual_output
    assert f"Job manifest file does not exist: {tmp_path}/job.yml" in actual_output
