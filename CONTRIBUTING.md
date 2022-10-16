# Contributing to pgq
## Bug reports
When reporting bugs, please add information about your operating system and Go version used to compile the code.

If you can provide a code snippet reproducing the issue, please do so.

## Code
Please write code that satisfies [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments) before submitting a pull-request.
Your code should be properly covered by extensive unit tests.

## Commit messages
Please follow the Go [commit messages](https://github.com/golang/go/wiki/CommitMessage) convention when contributing code.

## Running integration tests
The integration tests are located in a separate module inside the integration directory to avoid polluting the regular go.mod with dependencies only required to run the tests.

To run all tests:

```sh
# After running small tests
$ go test -v -race

# cd into the integration directory, and set the INTEGRATION_TESTDB environment variable as "true"
$ cd integration
$ INTEGRATION_TESTDB=true go test -v
```

To run them the tests you need to have PostgreSQL configured on your machine through the following environment variable:

| Environment Variable | Description |
| - | - |
| PostgreSQL environment variables | Please check https://www.postgresql.org/docs/current/libpq-envars.html |
| INTEGRATION_TESTDB | When running go test, database tests will only run if `INTEGRATION_TESTDB=true` |

Tests are safely run inside temporary databases created on-the-fly.
