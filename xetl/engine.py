import logging
import os

import yaml

from xetl.logging import LogContext, log_context
from xetl.models.command import Command
from xetl.models.job import Job, JobDataDirectoryNotFound
from xetl.models.task import Task, TaskFailure, UnknownTaskError, discover_tasks
from xetl.models.utils.dicts import conform_key

COMMANDS_REPO_PATH = os.path.abspath(os.path.dirname(__file__) + "/tasks")

logger = logging.getLogger(__name__)


class NoAliasDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


def _verify_data_dir(data_dir: str):
    if not os.path.exists(data_dir):
        logger.fatal(f"The job's `data` directory does not exist: {data_dir}")
        raise JobDataDirectoryNotFound


def execute_job(manifest_path: str, commands: list[str] | str | None = None, dryrun=False):
    if isinstance(commands, str):
        commands = commands.split(",")
    elif commands == []:
        logger.warning("No commands to execute")
        return
    if commands is not None:
        commands = [conform_key(name.strip()) for name in commands]

    job = Job.from_file(manifest_path)
    with log_context(LogContext.JOB, "Executing job: {}".format(job.name)):
        if dryrun:
            logger.info("Manifest parsed as:")
            for line in (
                yaml.dump(
                    job.model_dump(exclude_unset=True),
                    Dumper=NoAliasDumper,
                    sort_keys=False,
                )
                .strip()
                .split("\n")
            ):
                logger.info("  " + line)
        else:
            logger.info("Parsed manifest for job: {}".format(job.name))

        if tasks_repo_paths := job.tasks:
            logger.info(f"Discovering tasks at paths: {tasks_repo_paths}")
            available_tasks = discover_tasks(tasks_repo_paths)
            if not available_tasks:
                logger.error("Could not find any tasks at paths {}".format(tasks_repo_paths))
                return
        else:
            logger.warning("The property `tasks` is not defined in the job manifest, no tasks will be available")
            available_tasks = {}
        logger.info("Available tasks detected:")
        for cmd in available_tasks.values():
            logger.info(f" - {cmd.name}")

        filtered_commands = []
        for command in job.commands:
            if commands is None or command.name and conform_key(command.name) in commands:
                filtered_commands.append(command)
            else:
                logger.warning(f"Skipping command `{command.name}`")

        if not dryrun:
            _verify_data_dir(job.data)

        execute_job_commands(job.name, filtered_commands, available_tasks, dryrun)

        logger.info("Done! \\o/")


def execute_job_commands(job_name: str, commands: list[Command], tasks: dict[str, Task], dryrun: bool):
    for command in commands:
        task = _get_task(command, tasks)
        task.validate_inputs(command.env, critical_only=True)

    for i, command in enumerate(commands):
        if command.skip:
            logger.warning(f"Skipping command `{command.name or f'#{i + 1}'}` from job '{job_name}'")
            continue
        context_header = (
            f"Executing command {f'{i + 1}'} of {len(commands)}"
            if command.name is None
            else f"Executing command: {command.name} ({f'{i + 1}'} of {len(commands)})"
        )
        with log_context(LogContext.TASK, context_header):
            for line in yaml.dump(command.model_dump(), indent=2, sort_keys=False).strip().split("\n"):
                logger.info("  " + line)
            with log_context(LogContext.COMMAND, f"Executing task: {command.task}") as log_footer:
                returncode = _get_task(command, tasks).execute(command.env, dryrun)
                log_footer(f"Return code: {returncode}")
            if i < len(commands) - 1:
                logger.info("")  # leave a blank line between commands

        if returncode != 0:
            raise TaskFailure(returncode=returncode)


def _get_task(command: Command, tasks: dict[str, Task]) -> Task:
    task_name = command.task

    if task := tasks.get(task_name):
        return task
    else:
        raise UnknownTaskError(f"Unknown task `{task_name}`, should be one of: {sorted(tasks.keys())}")
