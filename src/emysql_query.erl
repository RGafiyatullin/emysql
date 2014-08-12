%% Copyright (c) 2013
%% Roman Gafiyatullin <r.gafiyatullin@me.com>
%% 
%% Permission is  hereby  granted,  free of charge,  to any person
%% obtaining  a copy of this software and associated documentation
%% files (the "Software"),to deal in the Software without restric-
%% tion,  including  without  limitation the rights to use,  copy, 
%% modify, merge,  publish,  distribute,  sublicense,  and/or sell
%% copies  of the  Software,  and to  permit  persons to  whom the
%% Software  is  furnished  to do  so,  subject  to the  following 
%% conditions:
%% 
%% The above  copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF  MERCHANTABILITY,  FITNESS  FOR  A  PARTICULAR  PURPOSE  AND
%% NONINFRINGEMENT. IN  NO  EVENT  SHALL  THE AUTHORS OR COPYRIGHT
%% HOLDERS  BE  LIABLE FOR  ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT,  TORT  OR OTHERWISE,  ARISING
%% FROM,  OUT OF OR IN CONNECTION WITH THE SOFTWARE  OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.

-module (emysql_query).
-export ([render/2]).

-spec render( Query :: iolist() | binary(), Args :: [ term() ] ) -> {ok, RenderedQuery :: iolist()}.
render( QueryIOL, Args ) ->
	Query = iolist_to_binary( [QueryIOL] ),
	render( Query, queue:new(), Args ).

-spec render( QuerySoFar :: binary(), QueryRendered :: queue:queue( term() ), Args :: [term()] ) -> {ok, RenderedQuery :: iolist()}.
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
