erlc hello.erl
erl -pa ebin/ -config log.config -boot start_sasl -s emysql start -s hello run -s init stop
