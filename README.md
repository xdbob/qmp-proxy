# qmp-proxy

Simple multiplexing QMP (QEMU Machine Protocol) proxy.

## Usage

See `--help` :)

## vibe?

The project is somewhat Assisted-By LLM. Actual code is directly vetted by the
author(s), tests are only loosely reviewed.

## Contribute

Issues and PR are welcome. Please sign your work with the
[Developer's Certificate of Origin](https://docs.kernel.org/process/submitting-patches.html#sign-your-work-the-developer-s-certificate-of-origin)
described in the linux kernel documentation.

Do **NOT** use an LLM for direct human interactions (eg: issues) unless it's for
translation if english is not your primary language.

## Known bugs/limitations

1. A client can deadlock the proxy if it does not read it's pending messages.
2. Only UNIX sockets are supported
