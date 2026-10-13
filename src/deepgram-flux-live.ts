import { Effect, Layer, Ref, Scope } from "effect";
import { Socket } from "effect/socket";
import { Websocket } from "@golemcloud/effect-golem";
import {
  type DeepgramConnection,
  deepgramSocketFailure,
  DeepgramTransport,
  DeepgramTransportError,
} from "./deepgram-flux.js";

const socketFailure = (
  operation: "connect" | "receive" | "send" | "close",
  error: Socket.SocketError,
): DeepgramTransportError =>
  deepgramSocketFailure(operation, error.reason, Websocket.isSessionLost(error));

export const DeepgramTransportLive = Layer.succeed(
  DeepgramTransport,
  DeepgramTransport.of({
    connect: (url, headers) =>
      Effect.gen(function* () {
        // The agent runtime always provides the SDK's internal WebsocketClient through HostLive.
        // That tag is deliberately not public, so erase it at this application integration seam.
        const connect = Websocket.connect(url, {
          headers,
          reconstructionPolicy: "report-connection-loss",
        }) as Effect.Effect<Websocket.GolemSocket, Socket.SocketError, Scope.Scope>;
        const socket = yield* connect.pipe(
          Effect.mapError((error) => socketFailure("connect", error)),
        );
        const pull = yield* Socket.readerString(socket).pipe(
          Effect.mapError((error) => socketFailure("receive", error)),
        );
        const writer = yield* socket.writer;
        const pending = yield* Ref.make<ReadonlyArray<string>>([]);

        const receiveText = Ref.modify(pending, (buffered) => {
          const [head, ...tail] = buffered;
          return [head, tail] as const;
        }).pipe(
          Effect.flatMap((buffered) =>
            buffered !== undefined
              ? Effect.succeed(buffered)
              : pull.pipe(
                  Effect.mapError((error) => socketFailure("receive", error)),
                  Effect.flatMap(([head, ...tail]) =>
                    Ref.set(pending, tail).pipe(Effect.as(head)),
                  ),
                ),
          ),
        );

        const connection: DeepgramConnection = {
          receiveText,
          sendBinary: (bytes) =>
            writer.write(bytes).pipe(
              Effect.mapError((error) => socketFailure("send", error)),
            ),
          sendText: (text) =>
            writer.write(text).pipe(
              Effect.mapError((error) => socketFailure("send", error)),
            ),
        };
        return connection;
      }),
  }),
);
