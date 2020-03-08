const
    io = require("socket.io-client"),
    ioClient = io.connect("http://localhost:8003");

ioClient.on("data", (msg) => {
  console.info(msg);}
);