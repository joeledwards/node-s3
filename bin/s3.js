#!/usr/bin/env node

function run () {
  const yargs = require('yargs')

  return yargs
    .commandDir('../commands')
    .demandCommand()
    .strict()
    .help()
    .argv
}

run()
