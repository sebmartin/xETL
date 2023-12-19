import os
import re
import subprocess
from textwrap import dedent
import pytest


@pytest.fixture
def transforms_repo_path(tmpdir):
    return tmpdir.mkdir("transforms")


@pytest.fixture
def output_dir(tmpdir):
    return tmpdir.mkdir("output")


def strip_dates(string):
    return re.sub(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+", "2023-11-23 21:36:52.983", string)


@pytest.fixture
def app_manifest(transforms_repo_path, output_dir, tmpdir):
    app = dedent(
        f"""
        name: test-app
        description: A test app to run end-to-end tests on
        data: {output_dir}
        transforms: {transforms_repo_path}
        env:
          APP_VAR: app-var-value
        jobs:
          main:
          - name: print-env
            transform: print-env
            env:
              INPUT1: 100
              INPUT2: false
              TEMP_FILE: ${{tmp.file}}
              OUTPUT: $data/env.txt
          - name: filter-env
            transform: filter
            env:
              FILE: ${{previous.OUTPUT}}
              PATTERN: -i input
              OUTPUT: $data/result.txt
        """
    )
    app_path = tmpdir / "app.yml"
    (app_path).write_text(app, encoding="utf-8")
    return app_path


@pytest.fixture
def print_env_transform(transforms_repo_path):
    print_env_transform = dedent(
        f"""
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
    print_env_transform_path = transforms_repo_path.mkdir("print-env") / "manifest.yml"
    (print_env_transform_path).write_text(print_env_transform, encoding="utf-8")
    return print_env_transform_path


@pytest.fixture
def filter_env_transform(transforms_repo_path):
    filter_env_transform = dedent(
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
    filter_env_transform_path = transforms_repo_path.mkdir("filter") / "manifest.yml"
    (filter_env_transform_path).write_text(filter_env_transform, encoding="utf-8")
    return filter_env_transform_path


def test_execute_bash_app(app_manifest, print_env_transform, filter_env_transform, output_dir, tmpdir):
    result = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "xetl",
            str(app_manifest),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    # Print the output if it wasn't successful
    assert result.returncode == 0, result.stdout.decode("utf-8")

    # Test resulting files
    assert os.path.exists(str(output_dir / "env.txt")), "The first step's file should have been created"
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
        Loading app manifest at: {data_dir}/app.yml
        ╭──╴Executing app: test-app ╶╴╴╶ ╶
        │ Parsed manifest for app: test-app
        │ Discovering transforms at paths: ['{data_dir}/transforms']
        │ Loading transform at: {data_dir}/transforms/print-env/manifest.yml
        │ Loading transform at: {data_dir}/transforms/filter/manifest.yml
        │ Available transforms detected:
        │  - print-env
        │  - filter
        ╔══╸Executing job: main ═╴╴╶ ╶
        ║ Executing step 1 of 2
        ║   name: print-env
        ║   description: null
        ║   transform: print-env
        ║   env:
        ║     APP_VAR: app-var-value
        ║     INPUT1: 100
        ║     INPUT2: false
        ║     TEMP_FILE: {tmp_file}
        ║     OUTPUT: {data_dir}/output/env.txt
        ║   skip: false
        ║┏━━╸Executing transform: print-env ━╴╴╶ ╶
        ║┃2023-11-23 21:36:52.983┊ WARNING Ignoring unknown env variable for transform `print-env`: APP_VAR. Valid names are: OUTPUT, TEMP_FILE, INPUT1, INPUT2
        ║┃2023-11-23 21:36:52.983┊ Temp values stored at {tmp_file}
        ║┃2023-11-23 21:36:52.983┊ {tmp_file}
        ║┗━━╸Return code: 0 ━╴╴╶ ╶
        ║{space}
        ║ Executing step 2 of 2
        ║   name: filter-env
        ║   description: null
        ║   transform: filter
        ║   env:
        ║     APP_VAR: app-var-value
        ║     FILE: {data_dir}/output/env.txt
        ║     PATTERN: -i input
        ║     OUTPUT: {data_dir}/output/result.txt
        ║   skip: false
        ║┏━━╸Executing transform: filter ━╴╴╶ ╶
        ║┃2023-11-23 21:36:52.983┊ WARNING Ignoring unknown env variable for transform `filter`: APP_VAR. Valid names are: FILE, PATTERN, OUTPUT
        ║┃2023-11-23 21:36:52.983┊ INPUT1=100
        ║┃2023-11-23 21:36:52.983┊ INPUT2=False
        ║┗━━╸Return code: 0 ━╴╴╶ ╶
        │ Done! \\o/
        """
    ).format(data_dir=str(tmpdir), space=" ", tmp_file=tmp_file)
    actual_result = result.stdout.decode("utf-8")
    assert strip_dates(actual_result.strip()) == strip_dates(expected_output.strip())


def test_execute_bash_app_dryrun(app_manifest, print_env_transform, filter_env_transform, output_dir, tmpdir):
    result = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "xetl",
            str(tmpdir / "app.yml"),
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
        Loading app manifest at: {data_dir}/app.yml
        ╭──╴Executing app: test-app ╶╴╴╶ ╶
        │ Manifest parsed as:
        │   name: test-app
        │   description: A test app to run end-to-end tests on
        │   data: {data_dir}/output
        │   env:
        │     APP_VAR: app-var-value
        │   transforms:
        │   - {data_dir}/transforms
        │   jobs:
        │     main:
        │     - name: print-env
        │       transform: print-env
        │       env:
        │         APP_VAR: app-var-value
        │         INPUT1: 100
        │         INPUT2: false
        │         TEMP_FILE: {tmp_file}
        │         OUTPUT: {data_dir}/output/env.txt
        │     - name: filter-env
        │       transform: filter
        │       env:
        │         APP_VAR: app-var-value
        │         FILE: {data_dir}/output/env.txt
        │         PATTERN: -i input
        │         OUTPUT: {data_dir}/output/result.txt
        │ Discovering transforms at paths: ['{data_dir}/transforms']
        │ Loading transform at: {data_dir}/transforms/print-env/manifest.yml
        │ Loading transform at: {data_dir}/transforms/filter/manifest.yml
        │ Available transforms detected:
        │  - print-env
        │  - filter
        ╔══╸Executing job: main ═╴╴╶ ╶
        ║ Executing step 1 of 2
        ║   name: print-env
        ║   description: null
        ║   transform: print-env
        ║   env:
        ║     APP_VAR: app-var-value
        ║     INPUT1: 100
        ║     INPUT2: false
        ║     TEMP_FILE: {tmp_file}
        ║     OUTPUT: {data_dir}/output/env.txt
        ║   skip: false
        ║┏━━╸Executing transform: print-env ━╴╴╶ ╶
        ║┃2023-11-23 21:36:52.983┊ WARNING Ignoring unknown env variable for transform `print-env`: APP_VAR. Valid names are: OUTPUT, TEMP_FILE, INPUT1, INPUT2
        ║┃2023-12-12 21:46:35.601┊ DRYRUN: Would execute with:
        ║┃2023-12-12 21:46:35.601┊   command: ['/bin/bash', '-c', 'echo "Temp values stored at $TEMP_FILE"\\n/usr/bin/env > $TEMP_FILE\\nls "$TEMP_FILE"\\ncat $TEMP_FILE > $OUTPUT\\n']
        ║┃2023-12-12 21:46:35.601┊   cwd: {data_dir}/transforms/print-env
        ║┃2023-12-12 21:46:35.601┊   env: APP_VAR=app-var-value, INPUT1=100, INPUT2=False, TEMP_FILE={tmp_file}, OUTPUT={data_dir}/output/env.txt
        ║┗━━╸Return code: 0 ━╴╴╶ ╶
        ║{space}
        ║ Executing step 2 of 2
        ║   name: filter-env
        ║   description: null
        ║   transform: filter
        ║   env:
        ║     APP_VAR: app-var-value
        ║     FILE: {data_dir}/output/env.txt
        ║     PATTERN: -i input
        ║     OUTPUT: {data_dir}/output/result.txt
        ║   skip: false
        ║┏━━╸Executing transform: filter ━╴╴╶ ╶
        ║┃2023-11-23 21:36:52.983┊ WARNING Ignoring unknown env variable for transform `filter`: APP_VAR. Valid names are: FILE, PATTERN, OUTPUT
        ║┃2023-12-12 21:46:35.602┊ DRYRUN: Would execute with:
        ║┃2023-12-12 21:46:35.603┊   command: ['/bin/bash', '-c', 'cat $FILE | grep $PATTERN | tee $OUTPUT']
        ║┃2023-12-12 21:46:35.603┊   cwd: {data_dir}/transforms/filter
        ║┃2023-12-12 21:46:35.603┊   env: APP_VAR=app-var-value, FILE={data_dir}/output/env.txt, PATTERN=-i input, OUTPUT={data_dir}/output/result.txt
        ║┗━━╸Return code: 0 ━╴╴╶ ╶
        │ Done! \\o/
        """
    ).format(data_dir=str(tmpdir), space=" ", tmp_file=tmp_file)
    actual_result = result.stdout.decode("utf-8")
    assert strip_dates(actual_result.strip()) == strip_dates(expected_output.strip())


def test_execute_with_failure(output_dir, transforms_repo_path, tmpdir):
    app = dedent(
        f"""
        name: test-app
        description: A test app to run end-to-end tests on
        data: {output_dir}
        transforms: {transforms_repo_path}
        jobs:
          main:
            - name: fail
              transform: fail
        """
    )
    app_path = tmpdir / "app.yml"
    (app_path).write_text(app, encoding="utf-8")

    filter_env_transform = dedent(
        """
        name: fail
        description: This is a transform that always fails
        env-type: bash
        run-command: cat /file/that/doesnt/exist
        """
    )
    filter_env_transform_path = transforms_repo_path.mkdir("filter") / "manifest.yml"
    (filter_env_transform_path).write_text(filter_env_transform, encoding="utf-8")

    result = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "xetl",
            str(tmpdir / "app.yml"),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    expected_return_code = 1
    assert result.returncode == expected_return_code, result.stdout.decode("utf-8")

    expected_output = dedent(
        """
        Loading app manifest at: {data_dir}/app.yml
        ╭──╴Executing app: test-app ╶╴╴╶ ╶
        │ Parsed manifest for app: test-app
        │ Discovering transforms at paths: ['{data_dir}/transforms']
        │ Loading transform at: {data_dir}/transforms/filter/manifest.yml
        │ Available transforms detected:
        │  - fail
        ╔══╸Executing job: main ═╴╴╶ ╶
        ║ Executing step 1 of 1
        ║   name: fail
        ║   description: null
        ║   transform: fail
        ║   env: {{}}
        ║   skip: false
        ║┏━━╸Executing transform: fail ━╴╴╶ ╶
        ║┃2023-11-23 21:36:52.983┊ cat: /file/that/doesnt/exist: No such file or directory
        ║┗━━╸Return code: {error_code} ━╴╴╶ ╶
        Transform failed, terminating job.
        """
    ).format(data_dir=str(tmpdir), space=" ", error_code=expected_return_code)
    actual_result = result.stdout.decode("utf-8")
    assert strip_dates(actual_result.strip()) == strip_dates(expected_output.strip())


def test_invalid_app_yaml(tmpdir):
    result = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "xetl",
            str(tmpdir / "app.yml"),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    actual_output = result.stdout.decode("utf-8")
    assert result.returncode == 1, actual_output
    assert f"File does not exist: {tmpdir}/app.yml" in actual_output
