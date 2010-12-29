module trioplax.Logger;

private import std.format;
private import std.c.stdio;
private import std.date;

import std.array: appender;

private import std.stdio;

//package
//{
//	Logger log;
//}

public class Logger
{
	private string trace_logfilename = "app.log";
	private FILE* ff;
	private string src = "";
	
	//	static this()
	//	{
	//		log = new Logger();
	//	}

	//	static ~this()
	//	{
	//		delete log;
	//	}

	this(string log_name, string _src)
	{
		trace_logfilename = log_name;
		ff = fopen(trace_logfilename.ptr, "aw");
		src = _src;
	}

	~this()
	{
		fclose(ff);;
	}

	void trace_io(bool io, byte* data, int len)
	{
		d_time now = getUTCtime();

		string str_io;

		if(io == true)
			str_io = "INPUT";
		else
			str_io = "OUTPUT";

		auto writer = appender!string();
		formattedWrite(writer, "\n\n[%04d-%02d-%02d %02d:%02d:%02d.%03d]\n%s:\n", yearFromTime(now), monthFromTime(now), dateFromTime(now),
				hourFromTime(now), minFromTime(now), secFromTime(now), msFromTime(now), str_io);

		writer.put(cast(char)0);

		fputs(cast(char*) writer.data, ff);

		for(int i = 0; i < len - 1; i++)
		{
			if (*data != 0)
				fputc(*data, ff);
			data++;
		}

		fflush(ff);
	}

	string trace(Char, A...)(in Char[] fmt, A args)
	{
		d_time now = getUTCtime();

		auto writer = appender!string();
		formattedWrite(writer, "[%04d-%02d-%02d %02d:%02d:%02d.%03d] [%s] ", yearFromTime(now), monthFromTime(now), dateFromTime(now),
				hourFromTime(now), minFromTime(now), secFromTime(now), msFromTime(now), src);		
		
		formattedWrite(writer, fmt, args);
		writer.put(cast(char)0);

		fputs(cast(char*) writer.data, ff);
		fputc('\r', ff);

		fflush(ff);
		return writer.data;
	}

	void trace_log_and_console(Char, A...)(in Char[] fmt, A args)
	{
		write(trace(fmt, args), "\n");
	}
}