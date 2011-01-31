module trioplax.triple;

import std.array;
import std.stdio;

public immutable byte _NONE = 0;
public immutable byte _RU = 1;
public immutable byte _EN = 2;

class List
{
	Appender!(Triple[]) lst;
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
		return S ~ " " ~ P ~ " " ~ O;
	}

}
