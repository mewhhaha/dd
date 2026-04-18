@0xea8d88ee7d24ff8f;

struct Header {
  name @0 :Text;
  value @1 :Text;
}

struct HttpRequest {
  method @0 :Text;
  url @1 :Text;
  headers @2 :List(Header);
  body @3 :Data;
  requestId @4 :Text;
}

struct HttpResponse {
  status @0 :UInt16;
  headers @1 :List(Header);
  body @2 :Data;
}

struct MethodCall {
  name @0 :Text;
  args @1 :Data;
  requestId @2 :Text;
}

struct MethodResult {
  value @0 :Data;
}

struct SocketMessage {
  sessionId @0 :Text;
  isText @1 :Bool;
  data @2 :Data;
}

struct SocketClose {
  sessionId @0 :Text;
  code @1 :UInt16;
  reason @2 :Text;
}

struct SocketOpen {
  sessionId @0 :Text;
  binding @1 :Text;
  key @2 :Text;
  requestId @3 :Text;
}

struct Target {
  workerName @0 :Text;
  binding @1 :Text;
  key @2 :Text;
}

struct InvokeRequest {
  target @0 :Target;
  union {
    fetch @1 :HttpRequest;
    method @2 :MethodCall;
  }
}

struct InvokeResponse {
  union {
    fetch @0 :HttpResponse;
    method @1 :MethodResult;
    error @2 :Text;
  }
}

struct WorkerSocketFrame {
  union {
    message @0 :SocketMessage;
    close @1 :SocketClose;
    open @2 :SocketOpen;
  }
}
