isort:
	python -m isort . --profile black
black:
	python -m black . --line-length=88
flake8:
	python -m flake8 . --max-line-length=88 \
					   --ignore=F401,E501,W503,F403,F405,F541,E731,F841
test:
	python -m pytest --no-header -v -s

clean:
	make isort
	make black
	make flake8
