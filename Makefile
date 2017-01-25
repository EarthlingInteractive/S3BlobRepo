default: run-unit-tests

.PHONY: run-unit-tests

composer.lock: | composer.json
	composer install

vendor: composer.lock composer.json
	composer install
	touch "$@"

run-unit-tests: vendor
	: # composer test ; takes forever; let's just run phpunit...
	vendor/bin/phpunit --bootstrap vendor/autoload.php test/
