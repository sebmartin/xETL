# 🙅‍♂️ETL
_/zɛtəl/_

[![CI](https://github.com/sebmartin/xetl/actions/workflows/ci.yml/badge.svg)](https://github.com/sebmartin/xETL/actions/workflows/ci.yml) [![codecov](https://codecov.io/gh/sebmartin/xETL/graph/badge.svg?token=8AFOOXA3AV)](https://codecov.io/gh/sebmartin/xETL)

## Overview

xETL is a versatile orchestration library for sequencing the execution of programs. While it *can* be used to build ETL (Extract, Transform, Load) pipelines, its simplicity and flexibility make it suitable for a wide range of tasks.

Its design is inspired by the following set of principles:

1. Minimize complexity by embracing the [Unix Philosophy](https://en.wikipedia.org/wiki/Unix_philosophy).
2. Maximize ease-of-use by reusing concepts from the [POSIX standards](https://en.wikipedia.org/wiki/POSIX) as much as possible.

The result is a simple yet powerful library that is easy to learn.

It is also unopiniated. The library itself is written in Python, but a job can be composed of tasks written in almost any language.

