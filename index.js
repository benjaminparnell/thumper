const meow = require('meow');
const amqplib = require('amqplib');
const bluebird = require('bluebird');
const fs = require('fs');
const path = require('path');

const cli = meow(
  `
  Usage
    $ thumper
    
  Options
    --queue, -q RabbitMQ queue
    --host, -h RabbitMQ host
    --path, -p Path to output files
`,
  {
    flags: {
      queue: {
        type: 'string',
        alias: 'q'
      },

      host: {
        type: 'string',
        alias: 'h'
      },

      path: {
        type: 'string',
        alias: 'p'
      }
    }
  }
);

function consumeMessage(channel, queueName, filePath) {
  return channel
    .get(queueName)
    .then(message => message.content.toString())
    .then(payload =>
      fs.writeFileSync(filePath, payload, {
        encoding: 'utf8'
      })
    );
}

function run(host, queueName, number, filePath) {
  amqplib
    .connect(host)
    .then(connection => connection.createChannel())
    .then(channel =>
      channel.assertQueue(queueName).then(queue => {
        const array = Array.from(
          { length: queue.messageCount },
          (value, index) => index + 1
        );

        return bluebird
          .each(array, index =>
            consumeMessage(
              channel,
              queueName,
              path.join(filePath, `${index}.txt`)
            )
          )
          .then(() => console.log('Done writing files'));
      })
    )
    .then(() => process.exit(0))
    .catch(error => {
      console.error(error);
      process.exit(1);
    });
}

run(cli.flags.host, cli.flags.queue, cli.flags.number, cli.flags.path);
