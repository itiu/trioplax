// D import file generated from 'src/trioplax/mongodb/ComplexKeys.d'
module trioplax.mongodb.ComplexKeys;
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
    override bool opEquals(Object o);

    override int opCmp(Object o);

    override string toString()
{
return cast(string)("{" ~ key1 ~ "}{" ~ key2 ~ "}{" ~ key3 ~ "}{" ~ key4 ~ "}");
}

    override hash_t toHash();

}
