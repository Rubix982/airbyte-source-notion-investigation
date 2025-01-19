# Airbyte Source Notion Investigation

This repository contains the code for the Airbyte Source Notion Investigation project.

The aim of this repository is to present the results of the investigation of the Source Notion connector, as there is a bug
that prevents fetching data for comments from Notion.a

## Pre-requisites

- Make sure you have the latest directory for the `source_notion` folder from the [Airbyte repository](https://github.com/airbytehq/airbyte/).
  - On the `master` branch, get the latest changes, and in the root of this repository, run the below copy command,
  ```sh
  cp -r PATH/airbyte/airbyte-integrations/connectors/source-notion/source_notion .
  ```
