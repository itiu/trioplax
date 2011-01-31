module trioplax.triple;

import std.array;
import std.stdio;

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

	this(string _S, string _P, string _O)
	{
		S = _S;
		P = _P;
		O = _O;
	}

	string toString()
	{
		return S ~ " " ~ P ~ " " ~ O;
	}

}
