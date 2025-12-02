# S3 Proxy with Caching

A high-performance S3-compatible proxy server written in Rust that forwards requests to a configurable S3-compatible backend while handling authentication and transparent caching.

## Motivation

Modern data processing engines like DuckDB and Polars leverage advanced query optimization techniques, including predicate pushdown, to only retrieve the data actually needed by queries. While this approach dramatically improves performance on local filesystems, it can generate hundreds or even thousands of small HTTP requests when working with object storage like S3. Each request carries significant HTTP overhead that doesn't exist with block-level filesystem access.

This S3 proxy is specifically designed to address this challenge by implementing aggressive caching strategies, particularly for HEAD operations and GET `range` requests. By reducing the round-trip latency and eliminating redundant requests to the backend, the proxy enables data processing engines to maintain their efficiency when working with cloud storage. Benchmarks have demonstrated that complex analytical queries can be accelerated by up to 500% when using the proxy compared to direct S3 access.

![Architecture Diagram](s3proxy.drawio.svg)

## Features

- **High Performance**: Built with Hyper and Tokio for async/await support
- **Authentication**: Handles AWS Signature V4 authentication with credential management
- **S3 Compatible**: Supports standard S3 operations (GET, PUT, DELETE, LIST)
- **Caching**: Built-in object size caching for improved performance
- **Cross-platform**: Supports cross-compilation for multiple architectures

## Quick Start

### Installation

Clone the repository:

```bash
git clone https://github.com/monken/s3proxy.git
cd s3proxy
```

Build the project:

```bash
cargo build --release
```

### Usage

Run the proxy with a target S3 endpoint:

```bash
# Using command line arguments
./target/release/s3proxy --endpoint https://s3.amazonaws.com --port 3000

# Using environment variables
export ENDPOINT=https://s3.amazonaws.com
export PORT=3000
./target/release/s3proxy
```

#### Configuration Options

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `--endpoint, -e` | `ENDPOINT` | Required | Target S3-compatible endpoint URL |
| `--port, -p` | `PORT` | `3000` | Port to listen on |

## Development

### Running in Development Mode

Start the development server with auto-reload:

```bash
cargo install cargo-make cargo-watch
cargo make dev
```

### Building

Standard build:
```bash
cargo build
```

Release build with optimizations:
```bash
cargo build --release
```

### Cross-compilation

The project supports cross-compilation using the `cross` tool:

```bash
cargo install cross
cross build --target x86_64-unknown-linux-gnu --release
```

## API Reference

The proxy supports standard S3 operations and forwards them to the configured endpoint:

### Supported Operations

- **GET Object**: `GET /{bucket}/{key}`
- **PUT Object**: `PUT /{bucket}/{key}`
- **DELETE Object**: `DELETE /{bucket}/{key}`
- **LIST Objects**: `GET /{bucket}?list-type=2`
- **HEAD Object**: `HEAD /{bucket}/{key}`

### Authentication

The proxy handles AWS Signature V4 authentication. Include standard AWS authentication headers in your requests:

- `Authorization`
- `X-Amz-Date`
- `X-Amz-Content-Sha256`

## Architecture

The proxy consists of several key components:

- **Router** (`src/router.rs`): HTTP request routing and parameter parsing
- **S3 Handler** (`src/s3_handler.rs`): Core S3 operation handling and request forwarding
- **Credentials Manager** (`src/credentials.rs`): Authentication and credential management
- **XML Writer** (`src/xml_writer.rs`): XML response formatting for S3 API responses

### Request Flow

1. Client sends S3 request to proxy
2. Router parses request and extracts bucket/key information
3. Credentials manager validates authentication
4. S3 Handler forwards signed request to target endpoint
5. Response is returned to client

## Configuration

### Environment Variables

Set up logging level:
```bash
export RUST_LOG=info
```

Configure the target endpoint:
```bash
export ENDPOINT=https://your-s3-endpoint.com
export PORT=8080
```

### Logging

The application uses structured logging with tracing. Configure log levels using the `RUST_LOG` environment variable:

```bash
# Debug level
RUST_LOG=debug ./s3proxy

# Info level (default)
RUST_LOG=info ./s3proxy

# Module-specific logging
RUST_LOG=s3proxy=debug,hyper=info ./s3proxy
```

## Performance Optimizations

The proxy includes several performance optimizations:

- **HTTP/1.1 Keep-alive**: TCP connection reuse with 60-second keepalive
- **Size Caching**: Object size caching to reduce HEAD requests
- **LTO and Strip**: Release builds use Link Time Optimization and symbol stripping
- **Async I/O**: Non-blocking I/O throughout the request pipeline

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow Rust best practices and idioms
- Add tests for new functionality
- Update documentation as needed
- Use `cargo fmt` for code formatting
- Run `cargo clippy` for linting

## Dependencies

Key dependencies include:

- **hyper**: HTTP server implementation
- **tokio**: Async runtime
- **reqwest**: HTTP client for backend requests
- **aws-sigv4**: AWS Signature V4 implementation
- **serde**: Serialization/deserialization
- **tracing**: Structured logging
- **clap**: Command-line argument parsing

## License

This project is licensed under the terms specified in the repository.

## Troubleshooting

### Common Issues

**Connection refused**
- Ensure the target endpoint is accessible
- Check network connectivity and firewall rules

**Authentication failures**
- Verify AWS credentials are properly configured
- Check that the target endpoint supports AWS Signature V4

**Performance issues**
- Monitor connection pooling and keep-alive settings
- Check target endpoint performance and limits

### Debug Logging

Enable debug logging to troubleshoot issues:

```bash
RUST_LOG=debug ./s3proxy --endpoint https://your-endpoint.com
```

## Support

For issues and questions:

1. Check the [Issues](../../issues) page
2. Review the troubleshooting section above
3. Create a new issue with detailed information