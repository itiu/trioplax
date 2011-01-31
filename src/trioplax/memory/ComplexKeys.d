module trioplax.memory.ComplexKeys;

import rt.util.hash;
import std.string;
import std.stdio;

class TwoKeys
{
	string key1;
	string key2;

	this(string _key1, string _key2)
	{
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

}

class ThreeKeys
{
	string key1;
	string key2;
	string key3;

	this(string _key1, string _key2, string _key3)
	{
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
}
