// D import file generated from 'src/trioplax/memory/ComplexKeys.d'
module trioplax.memory.ComplexKeys;
import rt.util.hash;
import std.string;
import std.stdio;
private import trioplax.Logger;

Logger log;
static this();
class FKeys
{
    string key1 = null;
    string key2 = null;
    string key3 = null;
    string key4 = null;
    byte count = 0;
    this(string _key1, string _key2 = null, string _key3 = null, string _key4 = null);
    override hash_t toHash()
{
hash_t hh = 0;
if (count > 0 && key1 !is null)
hh += hashOf(key1.ptr,key1.length,0);
if (count > 1 && key2 !is null)
hh += hashOf(key2.ptr,key2.length,0);
if (count > 2 && key3 !is null)
hh += hashOf(key3.ptr,key3.length,0);
if (count > 3 && key4 !is null)
hh += hashOf(key4.ptr,key4.length,0);
return hh;
}

    override bool opEquals(Object o);

    override int opCmp(Object o);

    override string toString()
{
return cast(string)("{" ~ key1 ~ "}{" ~ key2 ~ "}{" ~ key3 ~ "}{" ~ key4 ~ "}");
}

}
