# Generate export file for ExomAG Transfer

This tool will create an excel format file with fields formatted according to
ExomAG requirements.

Currently only Baserow-based databases are supported.

## Installation

python `>=3.11` is required. Install requirements into a suitable python
environment with the following command:

```
$ pip install -r requirements.txt
```

## Configuration

A baserow token needs to be provided. Create a `.secrets.toml` file in the
project directory with the following content:

```
baserow_token = "<INSERT TOKEN STRING>"
```

Instruction how to obtain a baserow token through the web interface can be found
in the [Baserow Manual](https://baserow.io/user-docs/personal-api-tokens).

Ensure that the token is sufficiently private by restricting permissions to 400:

```
$ chmod 400 .secrets.toml
```

## Usage

The following command can be used to create an excel file with the current date:

```
$ python -m exomag_transfer export_$(date -Idate).xlsx
```
