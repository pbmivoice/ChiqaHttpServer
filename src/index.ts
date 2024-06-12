import dotenv from 'dotenv';
import assert from 'assert';
import { IncomingMessage, createServer } from 'http';
import { BrokerClient, BrokerClientContext, Message, uuidv7 } from 'chiqa';

dotenv.config();
const { HTTP_HOSTNAME, HTTP_PORT, RESPONSE_TIMEOUT } = process.env;
const { CHIQA_HOSTNAME, CHIQA_PORT } = process.env;
assert(HTTP_HOSTNAME, 'HTTP_HOSTNAME is required');
assert(HTTP_PORT, 'HTTP_PORT is required');
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
  const server = createServer(async (req, res) => {
    const transaction = uuidv7();
    const responseTopic = { to, transaction, kind: 'http-response' };

    const timeout = setTimeout(() => {
      delete responses[transaction];
      res.writeHead(504);
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
  });

  server.listen(Number(HTTP_PORT), HTTP_HOSTNAME, () => {
    console.log(`HTTP on http://${HTTP_HOSTNAME}:${HTTP_PORT}/`);
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
