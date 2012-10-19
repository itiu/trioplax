// D import file generated from 'src/trioplax/Logger.d'
module trioplax.Logger;
private import std.format;

private import std.c.stdio;

import std.datetime;
import std.array : appender;
private import std.stdio;

private import std.datetime;

import std.c.linux.linux;
byte[1100] trace_msg;
version (X86_64)
{
    alias long _time;
}
else
{
    alias int _time;
}
public class Logger
{
    private int count = 0;

    private int prev_time = 0;

    private string trace_logfilename = "app";

    private string ext = "log";

    private FILE* ff = null;

    private string src = "";

    this(string log_name, string _ext, string _src)
{
trace_logfilename = log_name;
src = _src;
ext = _ext;
open_new_file();
}
    ~this()
{
fclose(ff);
}
    private void open_new_file()
{
count = 0;
_time tt = time(null);
tm* ptm = localtime(&tt);
int year = ptm.tm_year + 1900;
int month = ptm.tm_mon + 1;
int day = ptm.tm_mday;
int hour = ptm.tm_hour;
int minute = ptm.tm_min;
int second = ptm.tm_sec;
auto writer = appender!(string)();
formattedWrite(writer,"%s_%04d-%02d-%02d_%02d:%02d:%02d.%s",trace_logfilename,year,month,day,hour,minute,second,ext);
writer.put(cast(char)0);
if (ff !is null)
{
fflush(ff);
fclose(ff);
}
ff = fopen(writer.data.ptr,"aw");
}

    void trace_io(bool io, byte* data, ulong len);
    template trace(Char,A...)
{
string trace(in Char[] fmt, A args)
{
_time tt = time(null);
tm* ptm = localtime(&tt);
int year = ptm.tm_year + 1900;
int month = ptm.tm_mon + 1;
int day = ptm.tm_mday;
int hour = ptm.tm_hour;
int minute = ptm.tm_min;
int second = ptm.tm_sec;
auto now = Clock.currTime();
int usecs = now.fracSec.usecs;
count++;
if (prev_time > 0 && day != prev_time || count > 1000000)
{
open_new_file();
}
auto writer = appender!(string)();
if (src.length > 0)
formattedWrite(writer,"[%04d-%02d-%02d %02d:%02d:%02d.%03d] [%s] ",year,month,day,hour,minute,second,usecs,src);
else
formattedWrite(writer,"[%04d-%02d-%02d %02d:%02d:%02d.%03d] ",year,month,day,hour,minute,second,usecs);
formattedWrite(writer,fmt,args);
writer.put(cast(char)0);
fputs(cast(char*)writer.data,ff);
fputc('\x0a',ff);
fflush(ff);
prev_time = day;
return writer.data;
}
}
    template trace_log_and_console(Char,A...)
{
void trace_log_and_console(in Char[] fmt, A args)
{
write(trace(fmt,args),"\x0a");
}
}
}

