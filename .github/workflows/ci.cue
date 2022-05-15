name: "test"

on: pull_request: branches: ["*"]

jobs: {
	test: {
		"runs-on": "ubuntu-latest"
		steps: [{
			name: "Checkout"
			uses: "actions/checkout@v2"
			// }, {
			//  name: "Cache Go"
			//  uses: "actions/cache@v2"
			//  with: {
			//   path: """
			//    ~/.cache/go-build
			//    ~/.cache/pqx
			//    ~/go/pkg/mod
			//    """
			//   key: """
			//    go-${{ hashFiles('**/go.sum') }}
			//    """
			//   "restore-keys": """
			//    go-
			//    """
			//  }
		}, {
			name: "Setup Go"
			uses: "actions/setup-go@v2"
			with: {
				"go-version": "1.18.1"
			}
		}, {
			name: "Go Test"
			run: """
				go test -v ./...
				"""
		}]
	}
}
