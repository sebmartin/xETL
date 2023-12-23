import logging
import os
from pprint import pprint

import yaml

from xetl.logging import LogContext, log_context
from xetl.models.job import Job
from xetl.models.task import Task
from xetl.models.command import Command, CommandFailure, UnknownCommandError, discover_commands

COMMANDS_REPO_PATH = os.path.abspath(os.path.dirname(__file__) + "/commands")

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

        if commands_repo_paths := job.commands:
            logger.info(f"Discovering commands at paths: {commands_repo_paths}")
            commands = discover_commands(commands_repo_paths)
            if not commands:
                logger.error("Could not find any commands at paths {}".format(commands_repo_paths))
                return
        else:
            logger.warning("The property `commands` is not defined in the job manifest, no commands will be available")
            commands = {}
        logger.info(f"Available commands detected:")
        for t in commands.values():
            logger.info(f" - {t.name}")

        # Rudimentary implementation, a more robust implementation would consider a complex DAG of tasks
        if skip_to and job.tasks:
            while not job.tasks[0].name or job.tasks[0].name.lower() != skip_to.lower():
                logger.warning(f"Skipping task: {job.tasks[0].name or job.tasks[0].command}")
                del job.tasks[0]
        execute_job_tasks(job.name, job.tasks, commands, dryrun)

        logger.info("Done! \\o/")


def execute_job_tasks(job_name: str, tasks: list[Task], commands: dict[str, Command], dryrun: bool):
    for i, task in enumerate(tasks):
        # logger.info(f"Executing task {f'{i + 1}'} of {len(tasks)}")
        with log_context(LogContext.TASK, f"Executing task {f'{i + 1}'} of {len(tasks)}"):
            for line in yaml.dump(task.model_dump(), indent=2, sort_keys=False).strip().split("\n"):
                logger.info("  " + line)
            with log_context(LogContext.COMMAND, f"Executing command: {task.command}") as tail:
                if task.skip:
                    logger.warning(f"Skipping task `{task.name or f'#{i + 1}'}` from job '{job_name}'")
                    continue
                returncode = execute_job_task(task, commands, dryrun)
                tail(f"Return code: {returncode}")
            if i < len(tasks) - 1:
                logger.info("")  # leave a blank line between tasks

        if returncode != 0:
            raise CommandFailure(returncode=returncode)


def execute_job_task(task: Task, commands: dict[str, Command], dryrun) -> int:
    command_name = task.command

    if command := commands.get(command_name):
        return command.execute(task, dryrun)  # TODO: no unit tests hit this
    else:
        raise UnknownCommandError(f"Unknown command `{command_name}`, should be one of: {sorted(commands.keys())}")
