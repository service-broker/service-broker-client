# service-broker-client
Client library for interacting with a service broker

### API

#### Connect
```typescript
const sb = new ServiceBroker(websocketUrl, logger)
```

#### Advertise
```typescript
sb.advertise(
  service: {name: string, capabilities?: string[], priority?: number},
  handler: (req: Message) => Message|Promise<Message>
): void
```
A service provider calls this method to advertise to the broker the service(s) it provides.  For explanation of parameters, see [service broker](https://github.com/service-broker/service-broker).

#### SetServiceHandler
```typescript
sb.setServiceHandler(
  serviceName: string,
  handler: (req: Message) => Message|Promise<Message>
): void
```
This installs a handler for a particular service without advertising the it to the service broker.  This is for when the client knows the provider's endpointId and can send the request directly to it (see RequestTo).

#### Request
```typescript
sb.request(
  service: {name: string, capabilities?: string[]},
  req: Message,
  timeout?: number
): Promise<Message>
```
A client calls this method to request some service.  The broker will select a qualified provider based on `serviceName` and `capabilities`.  The parameter `req` contains the actual message that'll be delivered to the service provider (see `handler` in Advertise).  The returned promise contains the response from the provider.

#### RequestTo
```typescript
sb.requestTo(
  endpointId: string,
  serviceName: string,
  req: Message,
  timeout?: number
): Promise<Message>
```
Send a request directly to an endpoint.

#### Notify
```typescript
sb.notify(
  service: {name: string, capabilities?: string[]},
  msg: Message
): Promise<void>
```
A notification is a request without an `id`, and thus no response will be generated.  Use a notification when you don't care about the result.

#### NotifyTo
```typescript
sb.notifyTo(
  endpointId: string,
  serviceName: string,
  msg: Message
): Promise<void>
```
Send a notification directly to an endpoint.

#### Publish/Subscribe
```typescript
sb.publish(topic: string, text: string): Promise<void>
sb.subscribe(topic: string, handler: (text: string) => void): Promise<void>
```

#### Status
```typescript
sb.status(): Promise<any>
```
Returns the service broker's status, which includes the list of services and service providers currently active.

#### WaitEndpoint
```typescript
sb.waitEndpoint(endpointId: string): Promise<void>
```
Returns a promise that resolves when the specified endpoint disconnects.

#### Shutdown
```typescript
sb.shutdown(): Promise<void>
```
Close the connection to the service broker.
