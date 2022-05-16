name: "test"

on: pull_request: branches: ["*"]

jobs: {
	test: {
		"runs-on": "ubuntu-latest"
		steps: [{
			name: "Checkout"
			uses: "actions/checkout@v2"
		}, {
			name: "Setup Go"
			uses: "actions/setup-go@v3"
			with: {
				"go-version": "go1.18.2"
			}
		}, {
			name: "Cache Go & Postgres"
			uses: "actions/cache@v2"
			with: {
				path: """
					~/.cache/go-build
					~/.cache/pqx
					~/go/pkg/mod
					"""
				key: """
					go-${{ hashFiles('**/go.sum') }}
					"""
				"restore-keys": """
					go-
					"""
			}
		}, {
			name: "Go Test"
			run: """
				go test -v ./...
				"""
		}]
	}
}
