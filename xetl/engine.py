import logging
import os
from pprint import pprint

import yaml

from xetl.logging import LogContext, log_context
from xetl.models.job import Job
from xetl.models.command import Command
from xetl.models.task import Task, TaskFailure, UnknownTaskError, discover_tasks

COMMANDS_REPO_PATH = os.path.abspath(os.path.dirname(__file__) + "/tasks")

logger = logging.getLogger(__name__)


class NoAliasDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


def execute_job(manifest_path: str, skip_to: str | None = None, dryrun=False):
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
            tasks = discover_tasks(tasks_repo_paths)
            if not tasks:
                logger.error("Could not find any tasks at paths {}".format(tasks_repo_paths))
                return
        else:
            logger.warning("The property `tasks` is not defined in the job manifest, no tasks will be available")
            tasks = {}
        logger.info(f"Available tasks detected:")
        for cmd in tasks.values():
            logger.info(f" - {cmd.name}")

        # Rudimentary implementation, a more robust implementation would consider a complex DAG of commands
        if skip_to and job.commands:
            while not job.commands[0].name or job.commands[0].name.lower() != skip_to.lower():
                logger.warning(f"Skipping command: {job.commands[0].name or job.commands[0].task}")
                del job.commands[0]
        execute_job_commands(job.name, job.commands, tasks, dryrun)

        logger.info("Done! \\o/")


def execute_job_commands(job_name: str, commands: list[Command], tasks: dict[str, Task], dryrun: bool):
    for command in commands:
        task = _get_task(command, tasks)
        task.validate_inputs(command)

    for i, command in enumerate(commands):
        with log_context(LogContext.TASK, f"Executing command {f'{i + 1}'} of {len(commands)}"):
            for line in yaml.dump(command.model_dump(), indent=2, sort_keys=False).strip().split("\n"):
                logger.info("  " + line)
            with log_context(LogContext.COMMAND, f"Executing task: {command.task}") as tail:
                if command.skip:
                    logger.warning(f"Skipping command `{command.name or f'#{i + 1}'}` from job '{job_name}'")
                    continue
                returncode = _get_task(command, tasks).execute(command, dryrun)
                tail(f"Return code: {returncode}")
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
