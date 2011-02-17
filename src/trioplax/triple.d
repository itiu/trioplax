module trioplax.triple;

import std.array;
import std.stdio;
import std.array: appender;
private import std.format;

public immutable byte _NONE = 0;
public immutable byte _RU = 1;
public immutable byte _EN = 2;

class List
{
	bool[Triple] lst;
	
	Triple[] array ()
	{
		return lst.keys ();
	}

	void put (Triple tt)
	{
		lst[tt] = true;
	}
	
	string toString()
	{	
		return "@@@";
//		auto writer = appender!string();

//		formattedWrite(writer, "[%d]%s \n", array.length, array);
		
//		return writer.data;
	}
}

class Triple
{
	string S;
	string P;
	string O;

	byte lang;

	this()
	{
	}	
	
	this(string _S, string _P, string _O, byte _lang = _NONE)
	{
		S = cast(immutable)new char[_S.length];
		P = cast(immutable)new char[_P.length];
		O = cast(immutable)new char[_O.length];
		
		(cast(char[])S)[0..$] = _S[0..$];
		(cast(char[])P)[0..$] = _P[0..$];
		(cast(char[])O)[0..$] = _O[0..$];
		
//		S = _S;
//		P = _P;
//		O = _O;
		lang = _lang;
	}
		
	string toString()
	{
		string sS = S;
		string sP = P;
		string sO = O;
		
		if (sS is null)
			sS = "";
		
		if (sP is null)
			sP = "";
		
		if (sO is null)
			sO = "";
		
		return "<" ~ S ~ "><" ~ P ~ "><" ~ O ~ ">";
	}

}
