module trioplax.Log;

import std.stdio;

package
{
	Log log;
}

private
{
public class Log
{

static string trace_logfilename = "trace.log";
static FILE* fplog;

	static this()
	{
fplog = fopen(trace_logfilename.ptr, "w");
	}

	static ~this()
	{
	//	fplog.close;
		delete log;
	}

void trace (char[] tl, ...)
{
//_arguments, _argptr
        fprintf(fplog,"\t%.*s\n", "ewtwet");
}
 }
 
}

char[] fromStringz (char* src)
{
return null;
}