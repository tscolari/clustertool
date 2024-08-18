.PHONY: mocks
mocks:
	@find -name 'mock_*.go' -delete -not -path '**/*'
	@mockery --all --case underscore --exported
