# RabbitMQ CLI - Rust Implementation

A standalone Rust implementation of the RabbitMQ CLI that connects directly to RabbitMQ nodes using Erlang distribution protocol.

## Features

- **Direct Connection**: Connects to RabbitMQ nodes via Erlang distribution (no server-side changes needed)
- **Command Compatibility**: Executes the same commands as the Erlang-based CLI
- **Pluggable Transport**: Supports multiple connection methods (Erlang distribution, future WebSocket support)
- **Rich CLI Experience**: Better argument parsing, formatting, and interactive features
- **Cross-Platform**: Works on Linux, macOS, and Windows

## Architecture

This CLI replicates the behavior of `rabbit_cli_frontend.erl` but implemented in Rust:

1. **Frontend (Rust)**: Argument parsing, connection management, output formatting
2. **Transport Layer**: Erlang distribution protocol using `erl_dist` crate  
3. **Backend (Erlang)**: Existing RabbitMQ command handlers (unchanged)

```
┌─────────────────┐    erl_dist    ┌─────────────────┐
│   Rust CLI      │ ──────────────► │ RabbitMQ Node   │
│   Frontend      │                │ (Erlang)        │
└─────────────────┘                └─────────────────┘
```

## Installation

### From Source

```bash
cd rabbitmq-cli-rust
cargo build --release
sudo cp target/release/rabbitmq /usr/local/bin/
```

### Prerequisites

- **Erlang Cookie**: The CLI needs access to the Erlang cookie for authentication
  - Set `RABBITMQ_ERLANG_COOKIE` environment variable, or
  - Ensure `~/.erlang.cookie` is readable

## Usage

### Basic Commands

```bash
# List exchanges
rabbitmq list exchanges

# List queues  
rabbitmq list queues

# List bindings with filters
rabbitmq list bindings --source my-exchange
rabbitmq list bindings --destination my-queue

# Connect to specific node
rabbitmq --node rabbit@other-host list exchanges

# Verbose output
rabbitmq -vv list exchanges
```

### Global Options

- `--node, -n`: Specify RabbitMQ node to connect to
- `--verbose, -v`: Increase verbosity (can be used multiple times)
- `--help, -h`: Show help
- `--version, -V`: Show version

### Command Discovery

The CLI automatically discovers available commands from the connected RabbitMQ node, so it supports all commands implemented on the server side without needing updates.

## Configuration

### Erlang Cookie

The CLI needs the Erlang cookie for authentication. It looks for the cookie in:

1. `RABBITMQ_ERLANG_COOKIE` environment variable
2. `~/.erlang.cookie` file
3. System-wide locations (`/var/lib/rabbitmq/.erlang.cookie`, etc.)

### Connection

By default, the CLI tries to connect to `rabbit@<hostname>`. Override with:

```bash
rabbitmq --node rabbit@my-rabbitmq-server list exchanges
```

## Development

### Building

```bash
cargo build
```

### Testing

```bash
# Unit tests
cargo test

# Integration tests (requires running RabbitMQ)
SKIP_INTEGRATION_TESTS= cargo test
```

### Adding Transport Support

The transport layer is pluggable. To add WebSocket support:

1. Enable the `websocket` feature
2. Implement `WebSocketTransport` similar to `ErlangTransport`
3. Update connection logic in `cli.rs`

### Logging

Set `RUST_LOG` environment variable for debug output:

```bash
RUST_LOG=debug rabbitmq list exchanges
```

## Comparison with Erlang CLI

| Feature | Erlang CLI | Rust CLI |
|---------|------------|----------|
| Connection | Erlang distribution | Erlang distribution + future WebSocket |
| Commands | Built-in | Discovered from server |
| Performance | Good | Better (less overhead) |
| Memory Usage | Higher | Lower |
| Startup Time | Slower | Faster |
| Interactive Features | Basic | Enhanced (future) |
| Cross-Platform | Good | Better |

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

Same as RabbitMQ (Mozilla Public License 2.0)