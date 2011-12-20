// D import file generated from 'src/tango/core/Exception.d'
module tango.core.Exception;
class IllegalArgumentException : Exception
{
    this(char[] msg)
{
super(cast(string)msg);
}
    this(string msg)
{
super(msg);
}
}
