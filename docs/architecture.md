# Architecture of Places Sync

## Tech

- Built as a [Node.js](https://nodejs.org/en/) app
- Uses [dat](https://github.com/maxogden/dat) internally
- Can be triggered manually or with a scheduling tool like CRON

## Supported workflows

Places Sync supports one-way synchronization from a data source into the Places database. Each source must be setup at the `unit_code` level. Sync is currently **all or nothing**, meaning that a park has to choose one of two workflows:

1. Use Places Editor and the "seed" workflow to manage data in Places.
2. Turn on sync. This will disable all editing for the park in Places Editor.
