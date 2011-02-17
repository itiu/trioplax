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
	Appender!(Triple[]) lst;
	
	string toString()
	{		
		auto writer = appender!string();

		formattedWrite(writer, "[%d]%s \n", lst.data.length, lst.data);
		
		return writer.data;
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
		S = _S;
		P = _P;
		O = _O;
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
