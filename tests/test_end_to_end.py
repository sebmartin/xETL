import os
import re
import subprocess
from textwrap import dedent

import pytest


@pytest.fixture
def tasks_repo_path(tmpdir):
    return tmpdir.mkdir("tasks")


@pytest.fixture
def output_dir(tmpdir):
    return tmpdir.mkdir("output")


def strip_dates(string):
    return re.sub(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+", "2023-11-23 21:36:52.983", string)


@pytest.fixture
def job_manifest(tasks_repo_path, output_dir, tmpdir):
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
              OUTPUT: $data/env.txt
          - name: filter-env
            task: filter
            env:
              FILE: ${{previous.OUTPUT}}
              PATTERN: -i input
              OUTPUT: $data/result.txt
        """
    )
    job_path = tmpdir / "job.yml"
    (job_path).write_text(job, encoding="utf-8")
    return job_path


@pytest.fixture
def print_env_task(tasks_repo_path):
    print_env_task = dedent(
        """
        name: print-env
        description: Prints all env variables
        env-type: bash
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
        run-command: |
            echo "Temp values stored at $TEMP_FILE"
            /usr/bin/env > $TEMP_FILE
            ls "$TEMP_FILE"
            cat $TEMP_FILE > $OUTPUT
        """
    )
    print_env_task_path = tasks_repo_path.mkdir("print-env") / "manifest.yml"
    (print_env_task_path).write_text(print_env_task, encoding="utf-8")
    return print_env_task_path


@pytest.fixture
def filter_env_task(tasks_repo_path):
    filter_env_task = dedent(
        """
        name: filter
        description: Concatenate files listed in an input file
        env-type: bash
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
        run-command: cat $FILE | grep $PATTERN | tee $OUTPUT
        """
    )
    filter_env_task_path = tasks_repo_path.mkdir("filter") / "manifest.yml"
    (filter_env_task_path).write_text(filter_env_task, encoding="utf-8")
    return filter_env_task_path


def test_execute_bash_job(job_manifest, print_env_task, filter_env_task, output_dir, tmpdir):
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
        Loading job manifest at: {data_dir}/job.yml
        ╭──╴Executing job: test-job ╶╴╴╶ ╶
        │ Parsed manifest for job: test-job
        │ Discovering tasks at paths: ['{data_dir}/tasks']
        │ Loading task at: {data_dir}/tasks/print-env/manifest.yml
        │ Loading task at: {data_dir}/tasks/filter/manifest.yml
        │ Available tasks detected:
        │  - print-env
        │  - filter
        │ WARNING Ignoring unknown env variable for task `print-env`: JOB_VAR. Valid names are: OUTPUT, TEMP_FILE, INPUT1, INPUT2
        │ WARNING Ignoring unknown env variable for task `filter`: JOB_VAR. Valid names are: FILE, PATTERN, OUTPUT
        ┏━━╸Executing command 1 of 2 ━╴╴╶ ╶
        ┃   name: print-env
        ┃   description: null
        ┃   task: print-env
        ┃   env:
        ┃     JOB_VAR: job-var-value
        ┃     INPUT1: 100
        ┃     INPUT2: false
        ┃     TEMP_FILE: {tmp_file}
        ┃     OUTPUT: {data_dir}/output/env.txt
        ┃   skip: false
        ┃╭──╴Executing task: print-env ─╴╴╶ ╶
        ┃│2023-11-23 21:36:52.983┊ WARNING Ignoring unknown env variable for task `print-env`: JOB_VAR. Valid names are: OUTPUT, TEMP_FILE, INPUT1, INPUT2
        ┃│2023-11-23 21:36:52.983┊ Temp values stored at {tmp_file}
        ┃│2023-11-23 21:36:52.983┊ {tmp_file}
        ┃╰──╴Return code: 0 ─╴╴╶ ╶
        ┃{space}
        ┏━━╸Executing command 2 of 2 ━╴╴╶ ╶
        ┃   name: filter-env
        ┃   description: null
        ┃   task: filter
        ┃   env:
        ┃     JOB_VAR: job-var-value
        ┃     FILE: {data_dir}/output/env.txt
        ┃     PATTERN: -i input
        ┃     OUTPUT: {data_dir}/output/result.txt
        ┃   skip: false
        ┃╭──╴Executing task: filter ─╴╴╶ ╶
        ┃│2023-11-23 21:36:52.983┊ WARNING Ignoring unknown env variable for task `filter`: JOB_VAR. Valid names are: FILE, PATTERN, OUTPUT
        ┃│2023-11-23 21:36:52.983┊ INPUT1=100
        ┃│2023-11-23 21:36:52.983┊ INPUT2=False
        ┃╰──╴Return code: 0 ─╴╴╶ ╶
        │ Done! \\o/
        """
    ).format(data_dir=str(tmpdir), space=" ", tmp_file=tmp_file)
    actual_result = result.stdout.decode("utf-8")
    assert strip_dates(actual_result.strip()) == strip_dates(expected_output.strip())


def test_execute_bash_job_dryrun(job_manifest, print_env_task, filter_env_task, output_dir, tmpdir):
    result = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "xetl",
            str(tmpdir / "job.yml"),
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
        Loading job manifest at: {data_dir}/job.yml
        ╭──╴Executing job: test-job ╶╴╴╶ ╶
        │ Manifest parsed as:
        │   name: test-job
        │   description: A test job to run end-to-end tests on
        │   data: {data_dir}/output
        │   env:
        │     JOB_VAR: job-var-value
        │   tasks:
        │   - {data_dir}/tasks
        │   commands:
        │   - name: print-env
        │     task: print-env
        │     env:
        │       JOB_VAR: job-var-value
        │       INPUT1: 100
        │       INPUT2: false
        │       TEMP_FILE: {tmp_file}
        │       OUTPUT: {data_dir}/output/env.txt
        │   - name: filter-env
        │     task: filter
        │     env:
        │       JOB_VAR: job-var-value
        │       FILE: {data_dir}/output/env.txt
        │       PATTERN: -i input
        │       OUTPUT: {data_dir}/output/result.txt
        │ Discovering tasks at paths: ['{data_dir}/tasks']
        │ Loading task at: {data_dir}/tasks/print-env/manifest.yml
        │ Loading task at: {data_dir}/tasks/filter/manifest.yml
        │ Available tasks detected:
        │  - print-env
        │  - filter
        │ WARNING Ignoring unknown env variable for task `print-env`: JOB_VAR. Valid names are: OUTPUT, TEMP_FILE, INPUT1, INPUT2
        │ WARNING Ignoring unknown env variable for task `filter`: JOB_VAR. Valid names are: FILE, PATTERN, OUTPUT
        ┏━━╸Executing command 1 of 2 ━╴╴╶ ╶
        ┃   name: print-env
        ┃   description: null
        ┃   task: print-env
        ┃   env:
        ┃     JOB_VAR: job-var-value
        ┃     INPUT1: 100
        ┃     INPUT2: false
        ┃     TEMP_FILE: {tmp_file}
        ┃     OUTPUT: {data_dir}/output/env.txt
        ┃   skip: false
        ┃╭──╴Executing task: print-env ─╴╴╶ ╶
        ┃│2023-11-23 21:36:52.983┊ WARNING Ignoring unknown env variable for task `print-env`: JOB_VAR. Valid names are: OUTPUT, TEMP_FILE, INPUT1, INPUT2
        ┃│2023-12-12 21:46:35.601┊ DRYRUN: Would execute with:
        ┃│2023-12-12 21:46:35.601┊   task: ['/bin/bash', '-c', 'echo "Temp values stored at $TEMP_FILE"\\n/usr/bin/env > $TEMP_FILE\\nls "$TEMP_FILE"\\ncat $TEMP_FILE > $OUTPUT\\n']
        ┃│2023-12-12 21:46:35.601┊   cwd: {data_dir}/tasks/print-env
        ┃│2023-12-12 21:46:35.601┊   env: JOB_VAR=job-var-value, INPUT1=100, INPUT2=False, TEMP_FILE={tmp_file}, OUTPUT={data_dir}/output/env.txt
        ┃╰──╴Return code: 0 ─╴╴╶ ╶
        ┃{space}
        ┏━━╸Executing command 2 of 2 ━╴╴╶ ╶
        ┃   name: filter-env
        ┃   description: null
        ┃   task: filter
        ┃   env:
        ┃     JOB_VAR: job-var-value
        ┃     FILE: {data_dir}/output/env.txt
        ┃     PATTERN: -i input
        ┃     OUTPUT: {data_dir}/output/result.txt
        ┃   skip: false
        ┃╭──╴Executing task: filter ─╴╴╶ ╶
        ┃│2023-11-23 21:36:52.983┊ WARNING Ignoring unknown env variable for task `filter`: JOB_VAR. Valid names are: FILE, PATTERN, OUTPUT
        ┃│2023-12-12 21:46:35.602┊ DRYRUN: Would execute with:
        ┃│2023-12-12 21:46:35.603┊   task: ['/bin/bash', '-c', 'cat $FILE | grep $PATTERN | tee $OUTPUT']
        ┃│2023-12-12 21:46:35.603┊   cwd: {data_dir}/tasks/filter
        ┃│2023-12-12 21:46:35.603┊   env: JOB_VAR=job-var-value, FILE={data_dir}/output/env.txt, PATTERN=-i input, OUTPUT={data_dir}/output/result.txt
        ┃╰──╴Return code: 0 ─╴╴╶ ╶
        │ Done! \\o/
        """
    ).format(data_dir=str(tmpdir), space=" ", tmp_file=tmp_file)
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
        env-type: bash
        run-command: cat /file/that/doesnt/exist
        """
    )
    filter_env_task_path = tasks_repo_path.mkdir("filter") / "manifest.yml"
    (filter_env_task_path).write_text(filter_env_task, encoding="utf-8")

    result = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "xetl",
            str(tmpdir / "job.yml"),
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
        ┏━━╸Executing command 1 of 1 ━╴╴╶ ╶
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


def test_invalid_job_yaml(tmpdir):
    result = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "xetl",
            str(tmpdir / "job.yml"),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    actual_output = result.stdout.decode("utf-8")
    assert result.returncode == 1, actual_output
    assert f"Job manifest file does not exist: {tmpdir}/job.yml" in actual_output
