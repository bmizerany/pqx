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
		}, {
			name: "Cache Go"
			uses: "actions/cache@v2"
			with: {
				path: """
					~/.cache/go-build
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
			name: "Setup Go"
			uses: "actions/setup-go@v2"
			with: {
				"go-version": "1.18.1"
			}
		}, {
			name: "Setup Postgres"
			if:   "steps.cache-postgres.outputs.cache-hit != 'true'"
			run: """
				sudo apt-get install -y postgresql
				echo /usr/lib/postgresql/12/bin >> $GITHUB_PATH
				"""
		}, {
			name: "Go Test"
			run: """
				sudo -u postgres sh -c "
					export PQX_D=0 # leave; set >0 if debugging is needed
					export PATH="$PATH" # copy PATH from parent shell user
					go test -v ./...
				"
				"""
		}]
	}
}
