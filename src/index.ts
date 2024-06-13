import dotenv from 'dotenv';
import assert from 'assert';
import { resolve } from 'path';
import { readFileSync } from 'fs';
import { IncomingMessage, createServer as http } from 'http';
import { createServer as https } from 'https';
import { BrokerClient, BrokerClientContext, Message, uuidv7 } from 'chiqa';

dotenv.config();
const { HTTP_HOSTNAME, HTTP_PORT, HTTPS_PORT, CERTS_DIR } = process.env;
const { CHIQA_HOSTNAME, CHIQA_PORT, RESPONSE_TIMEOUT } = process.env;
assert(HTTP_HOSTNAME, 'HTTP_HOSTNAME is required');
assert(HTTP_PORT, 'HTTP_PORT is required');
assert(HTTPS_PORT, 'HTTPS_PORT is required');
assert(CERTS_DIR, 'CERTS_DIR is required');
assert(CHIQA_HOSTNAME, 'CHIQA_HOSTNAME is required');
assert(CHIQA_PORT, 'CHIQA_PORT is required');
assert(RESPONSE_TIMEOUT, 'RESPONSE_TIMEOUT is required');

const name = 'ChiqaHttpServer';
const [from, to] = [name, name] as const;

export type ResponsePayload = {
  status?: number;
  headers?: Record<string, string>;
  body: string;
};

function getBody(request: IncomingMessage) {
  return new Promise<string>(resolve => {
    const bodyParts: Buffer[] = [];
    request
      .on('data', chunk => {
        bodyParts.push(chunk);
      })
      .on('end', () => {
        const body = Buffer.concat(bodyParts).toString();
        resolve(body);
      });
  });
}

const responses: Record<string, (message: Message) => void> = {};

const onReady = (ctx: BrokerClientContext) => {
  http((req, res) => {
    const Location = 'https://' + req.headers['host'] + req.url;
    res.writeHead(301, { Location });
    res.end();
  }).listen(Number(HTTP_PORT), () => {
    console.log(`HTTP on http://${HTTP_HOSTNAME}:${HTTP_PORT}/`);
  });

  const certsDir = resolve(CERTS_DIR);
  const server = https(
    {
      key: readFileSync(`${certsDir}/privkey.pem`),
      cert: readFileSync(`${certsDir}/fullchain.pem`),
      ca: readFileSync(`${certsDir}/chain.pem`),
    },
    async (req, res) => {
      const transaction = uuidv7();
      const responseTopic = { to, transaction, kind: 'http-response' };

      const timeout = setTimeout(() => {
        delete responses[transaction];
        res.writeHead(504);
        res.end();
      }, Number(RESPONSE_TIMEOUT));

      responses[transaction] = (message: Message) => {
        clearTimeout(timeout);
        const { status, headers, body } = message.payload as ResponsePayload;
        if (status && !headers) {
          res.writeHead(status, { 'Content-Type': 'application/json' });
        }
        if (headers) {
          res.writeHead(status ?? 200, headers);
        }
        res.write(JSON.stringify(body));
        res.end();
      };

      ctx.subscribe({ match: 'has all', keys: responseTopic });

      const body = await getBody(req);

      ctx.send({
        topic: { from, transaction, kind: 'http-request' },
        payload: {
          method: req.method,
          url: req.url,
          headers: req.headers,
          body,
        },
        subMessage: { topic: responseTopic },
      });
    },
  );

  server.listen(Number(HTTPS_PORT), HTTP_HOSTNAME, () => {
    console.log(`HTTPS on https://${HTTP_HOSTNAME}:${HTTPS_PORT}/`);
  });
};

const onMessage = async (message: Message) => {
  const { transaction, kind } = message.topic;
  if (!transaction || kind !== 'http-response') return;
  responses[transaction]?.(message);
  delete responses[transaction];
};

BrokerClient(
  `ws://${CHIQA_HOSTNAME}:${Number(CHIQA_PORT)}`,
  onReady,
  onMessage,
  x => console.error(x),
);
