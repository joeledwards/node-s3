const app = require('@buzuli/app')
const aws = require('@buzuli/aws')

module.exports = handler

function handler (entry) {
  return options => {
    app({
      modules: {
        aws: () => aws.resolve()
      }
    })(async ({
      modules: {
        aws
      }
    }) => {
      await entry({
        aws,
        options
      })
    })
  }
}
