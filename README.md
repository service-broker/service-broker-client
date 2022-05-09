# service-broker-client
Client library for interacting with a service broker


### Connect
```typescript
const sb = new ServiceBroker("ws://sb.mydomain.com:8080", logger)
```


### Advertise
```typescript
sb.advertise(
  service: {name: string, capabilities?: string[], priority?: number},
  handler: (req: Message) => Message|Promise<Message>
)
```
A service provider calls this method to advertise to the broker the service(s) it provides.  For explanation of parameters, see [service broker](https://github.com/service-broker/service-broker).


### Request
```typescript
sb.request(
  service: {name: string, capabilities?: string[]},
  req: Message,
  timeout?: number
)
: Promise<Message>
```
A client calls this method to request some service.  The broker will select a qualified provider based on service `name` and `capabilities`.  The parameter `req` contains the actual message that'll be delivered to the service provider (see `handler` in Advertise).  The returned promise contains the response from the provider.


### RequestTo
```typescript
sb.requestTo(
  endpointId: string,
  serviceName: string,
  req: Message,
  timeout?: number
)
: Promise<Message>
```
Send a request directly to an endpoint.


### Notify
```typescript
sb.notify(
  service: {name: string, capabilities?: string[]},
  msg: Message
)
```
A notification is a request without an `id`, and thus no response will be generated.  Use a notification when you don't care about the result.


### NotifyTo
```typescript
sb.notifyTo(
  endpointId: string,
  serviceName: string,
  msg: Message
)
```
Send a notification directly to an endpoint.


### Publish/Subscribe
```typescript
sb.publish(
  topic: string,
  text: string
)

sb.subscribe(
  topic: string,
  handler: (text: string) => void
)
```


### SetServiceHandler
```typescript
sb.setServiceHandler(
  serviceName: string,
  handler: (req: Message) => Message|Promise<Message>
)
```
This installs a handler for a particular service without advertising the it to the broker.  This is for when the client knows the provider's endpointId and can send the request directly to it (see RequestTo).


### Status
```typescript
sb.status(): Promise<any>
```
Returns the service broker's status, which includes the list of services and service providers currently active.


### WaitEndpoint
```typescript
sb.waitEndpoint(endpointId: string): Promise<void>
```
Returns a promise that resolves when the specified endpoint disconnects.


### Shutdown
```typescript
sb.shutdown()
```
Close the connection to the service broker.
