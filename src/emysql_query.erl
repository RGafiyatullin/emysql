-module (emysql_query).
-export ([render/2]).

-spec render( Query :: iolist() | binary(), Args :: [ term() ] ) -> {ok, RenderedQuery :: iolist()}.
render( QueryIOL, Args ) ->
	Query = iolist_to_binary( [QueryIOL] ),
	render( Query, queue:new(), Args ).

-spec render( QuerySoFar :: binary(), QueryRendered :: queue(), Args :: [term()] ) -> {ok, RenderedQuery :: iolist()}.
render( <<>>, _QueryRendered, [ _ | _ ] ) -> {error, too_many_args};
render( QuerySoFar, QueryRendered, [] ) -> {ok, [ queue:to_list( QueryRendered ), QuerySoFar ]};
render( << $?/utf8, QuerySoFar/binary >>, QueryRendered, [ Arg | Args ] ) ->
	ArgL = arg_literal( Arg ),
	render( QuerySoFar, queue:in( ArgL, QueryRendered ), Args );

render( << Quot/utf8, QuerySoFar/binary >>, QueryRendered, Args )
	when Quot == $'
	% orelse Quot == $"
	orelse Quot == $`
->
	case skip_quot( Quot, QuerySoFar, queue:in( Quot, QueryRendered ) ) of
		{ok, QuerySoFar1, QueryRendered1} -> render( QuerySoFar1, QueryRendered1, Args);
		{error, Reason} -> {error, Reason}
	end;

render( << Char/utf8, QuerySoFar/binary >>, QueryRendered, Args ) -> render( QuerySoFar, queue:in( Char, QueryRendered ), Args ).

arg_literal( Val ) -> emysql_util:encode( Val, true ).

skip_quot( Quot, << Quot/utf8, QuerySoFar/binary >>, QueryRendered ) ->	{ ok, QuerySoFar, queue:in( Quot, QueryRendered ) };
skip_quot( Quot, << Char/utf8, QuerySoFar/binary >>, QueryRendered ) -> skip_quot( Quot, QuerySoFar, queue:in( Char, QueryRendered ) );
skip_quot( Quot, <<>>, _QueryRendered ) -> {error, {quot_mismatch, Quot}}.
