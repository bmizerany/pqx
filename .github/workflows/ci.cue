name: "test"
on: {
	push: {
		branches: ["main"]
	}
	pull_request: null
}
jobs: {
	test: {
		"runs-on": "ubuntu-latest"
		steps: [{
			name: "Checkout"
			uses: "actions/checkout@v2"
			with: {
				"fetch-depth": 1
			}
		}, {
			name: "Setup Go"
			uses: "actions/setup-go@v2"
			with: {
				"go-version": "1.17"
			}
		}, {
			name: "Setup Postgres"
			run: """
				sudo apt-get install -y postgresql
			"""
		}, {
			name: "Go Test"
			env: PQX_D:   "5"
			run: """
				sudo -u postgres sh -c "
					export PATH="/usr/lib/postgresql/12/bin:$PATH"
					go test -v ./...
				"
				"""
		}]
	}
}