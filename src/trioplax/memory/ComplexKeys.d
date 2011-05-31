module trioplax.memory.ComplexKeys;

import rt.util.hash;
import std.string;
import std.stdio;

private import trioplax.Logger;

Logger log;

static this()
{
	log = new Logger("trioplax", "log", "");
}

class TwoKeys
{
	string key1;
	string key2;

	this(string _key1, string _key2)
	{
		//		key1 = new char[_key1.length];
		//		key2 = new char[_key2.length];

		//		key1[0 .. $] = _key1[0 .. $];
		//		key2[0 .. $] = _key2[0 .. $];

		key1 = _key1;
		key2 = _key2;
	}

	hash_t toHash()
	{
		return hashOf(key1.ptr, key1.length, 0) + hashOf(key2.ptr, key2.length, 0);
	}

	bool opEquals(Object o)
	{
		TwoKeys f = cast(TwoKeys) o;

		if((this.key1.ptr == f.key1.ptr) && (this.key2.ptr == f.key2.ptr))
			return true;

		return (std.string.cmp(this.key1, f.key1) == 0 && std.string.cmp(this.key2, f.key2) == 0);
	}

	int opCmp(Object o)
	{
		TwoKeys f = cast(TwoKeys) o;
		if(!f)
			return -1;

		if(std.string.cmp(this.key1, f.key1) == 0)
			return std.string.cmp(this.key2, f.key2);

		return std.string.cmp(this.key1, f.key1);
	}

	string toString()
	{
		return cast(immutable) ("{" ~ key1 ~ "}{" ~ key2 ~ "}");
	}

}

class ThreeKeys
{
	string key1;
	string key2;
	string key3;

	this(string _key1, string _key2, string _key3)
	{
		//		key1 = new char[_key1.length];
		//		key2 = new char[_key2.length];
		//		key3 = new char[_key3.length];

		//		key1[0 .. $] = _key1[0 .. $];
		//		key2[0 .. $] = _key2[0 .. $];
		//		key3[0 .. $] = _key3[0 .. $];

		key1 = _key1;
		key2 = _key2;
		key3 = _key3;
	}

	hash_t toHash()
	{
		return hashOf(key1.ptr, key1.length, 0) + hashOf(key2.ptr, key2.length, 0) + hashOf(key3.ptr, key3.length, 0);
	}

	bool opEquals(Object o)
	{
		ThreeKeys f = cast(ThreeKeys) o;

		if((this.key1.ptr == f.key1.ptr) && (this.key2.ptr == f.key2.ptr) && (this.key3.ptr == f.key3.ptr))
			return true;

		log.trace("opEquals");

		return (std.string.cmp(this.key1, f.key1) == 0 && std.string.cmp(this.key2, f.key2) == 0 && std.string.cmp(this.key3, f.key3) == 0);
	}

	int opCmp(Object o)
	{
		// не ясно как сравнивать три ключа для получения результата -1 0 +1

		ThreeKeys f = cast(ThreeKeys) o;
		if(!f)
			return -1;

		int c1 = std.string.cmp(this.key1, f.key1);
		int c2 = std.string.cmp(this.key2, f.key2);
		int c3 = std.string.cmp(this.key3, f.key3);

		if(c1 == c2 && c2 == c3 && c3 == c1)
			return c1;

		if(c1 == -1 || c2 == -1 || c3 == -1)
			return -1;
		else
			return 1;
	}

	string toString()
	{
		return cast(immutable) ("{" ~ key1 ~ "}{" ~ key2 ~ "}{" ~ key3 ~ "}");
	}

}

class FourKeys
{
	string key1;
	string key2;
	string key3;
	string key4;

	this(string _key1, string _key2, string _key3, string _key4)
	{
		//		key1 = new char[_key1.length];
		//		key2 = new char[_key2.length];
		//		key3 = new char[_key3.length];
		//		key4 = new char[_key4.length];

		//		key1[0 .. $] = _key1[0 .. $];
		//		key2[0 .. $] = _key2[0 .. $];
		//		key3[0 .. $] = _key3[0 .. $];
		//		key4[0 .. $] = _key4[0 .. $];

		key1 = _key1;
		key2 = _key2;
		key3 = _key3;
		key4 = _key4;
	}

	hash_t toHash()
	{
		return hashOf(key1.ptr, key1.length, 0) + hashOf(key2.ptr, key2.length, 0) + hashOf(key3.ptr, key3.length, 0) + hashOf(key4.ptr,
				key4.length, 0);
	}

	bool opEquals(Object o)
	{
		FourKeys f = cast(FourKeys) o;

		if((this.key1.ptr == f.key1.ptr) && (this.key2.ptr == f.key2.ptr) && (this.key3.ptr == f.key3.ptr) && (this.key4.ptr == f.key4.ptr))
			return true;

		return (std.string.cmp(this.key1, f.key1) == 0 && std.string.cmp(this.key2, f.key2) == 0 && std.string.cmp(this.key3, f.key3) == 0 && std.string.cmp(
				this.key4, f.key4) == 0);
	}

	int opCmp(Object o)
	{
		// не ясно как сравнивать 4 ключа для получения результата -1 0 +1

		FourKeys f = cast(FourKeys) o;
		if(!f)
			return -1;

		int c1 = std.string.cmp(this.key1, f.key1);
		int c2 = std.string.cmp(this.key2, f.key2);
		int c3 = std.string.cmp(this.key3, f.key3);
		int c4 = std.string.cmp(this.key4, f.key4);

		if(c1 == c2 && c2 == c1 && c3 == c1 && c4 == c1)
			return c1;

		if(c1 == -1 || c2 == -1 || c3 == -1 || c4 == -1)
			return -1;
		else
			return 1;
	}

	string toString()
	{
		return cast(immutable) ("{" ~ key1 ~ "}{" ~ key2 ~ "}{" ~ key3 ~ "}{" ~ key4 ~ "}");
	}
}
