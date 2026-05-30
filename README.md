# TCP Echo Server in Async and Sync Architectures

## Usage
In order to run each program, since we have a crate workspace, you must specify the workspace to run when using cargo run with the -p flag. 

```
cargo run -p echo-sync -- --addr <addr> --port <port>

cargo run -p echo-async -- --addr <addr> --port <port> --max-conns <N> --stats
```

Every flag is optional, below are the defaults:

- `--addr` => `127.0.0.1`
- `--port` => `7878`
- `--max-conns` => `0`
  - note: max-conns must be greater than 0.
- `--stats` => false

## How shutdown works

### Sync
In the sync architecture, shutdown is handled by a simple `Arc<AtomicBool>` that the server thread and each connection is watching. Upon setting it to true, the server loop breaks and any threads that were created are joined. To achieve this, the server sets the listener to be non-blocking so in the main server loop, we catch the WouldBlock error and sleep 100ms so as not to poll too many times. Then, when the shutdown is set to true, each connection thread is given 5 seconds to await a timeout using set_read_timeout on the stream, and when that throws the TimeOut or WouldBlock error, that loop then closes the connection to the socket. This is done to allow for connected sockets to have time to finish their inputs since we don't have access to an await like in the async design.

### Async
In the async architecture, our main loop is handled by a tokio::select! which awaits the different futures we have set up. One such future is listener.accept which takes in new connections and spawns a new task per connection, giving them access to a shutdown receiver which is used to gracefully close those tasks. Another future we are awaiting is the shutdown handler. When ctrlc is pressed (or sigint/sigterm) is received, the shutdown process starts by breaking out of the server loop and joining the tasks in our task pool (`tasks`).

## General Reflection
 
This project has us writing a tcp echo server using synchronous (echo-sync) and asynchronous (echo-async) design patterns. The project showcases the differences between how sync and async design is used in an asynchronous-by-nature server/client environment.

During the development of the sync server, it really reminded me of learning TCP sockets in Java back in college, as the style of implementation is similar, using std::thread to spawn a new thread to handle the server as well as incoming connections to the server. It was also painful having to utilize a sleep to constantly check for incoming connections or for the shutdown handler to fire. It was painful because sleeps were always taught to me to be code smells.

## Points of reflection

1. UNIX only SIGTERM handler

While writing the shutdown logic for the async server, I figured it would be a good moment to learn more about tokio::select! so I decided to implement a shutdown handler for the SIGTERM system call. It started with only having an extra branch in the tokio::select! to handle the signal but I quickly realized this would not fly since I also programmed in Windows. This was something that I did wrong at first and had to correct. See, on Windows, SIGTERM didn't exist (hence the package coming from tokio::signal::unix) so it would not compile. This lead to me extracting the tokio::signal::ctrl_c() and the sigterm.recv() branches into its own shutdown join handler that could utilize #[cfg(unix)] to determine whether the SIGTERM logic would be built into the binary.

2. --stats

While learning about tokio::select!, --stats flag was the other part of me learning how to add branches to the tokio::select!. I had to learn how to use tokio::time::interval to handle periodic updates that the tokio::select! could await. I figured it would be good to implement since in a production environment, I may want to utilize this feature to handle something like a health check on the server, or other stats logging (more than just # of users connected).

3. lib.rs and how to export modules for integration tests

This was my first time learning how to write integration tests so I was able to decide how to refactor my initial solution to easily enable integration tests to have access to the server functions (specifically `start_server`). Originally, I just used mod server; in the main.rs file to be able to access the run_server function in server.rs, but had to figure out a way to publically expose this function in my crate. Hence I found lib.rs and it made sharing the server module practically free using pub mod server;

4. Difference between sync/async architecture for handling incoming connections and awaiting the shutdown handler. 

I found, while writing the async implementation, that handling futures were much more straight forward and readable using async/futures than in the sync implementation. Having to use thread::sleep while polling for incoming connections or catching the shutdown signal felt like a real code smell that was easily handled in the async implementation. I guess tcp servers are async by nature so it only makes sense that having a framework for async tasks makes it read much better than in the traditional threading framework.
