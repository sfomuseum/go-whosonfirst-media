package common

/*

You might be thinking: I know, I'll make a common pool of buckets that all the
codes can use! It's okay, I thought that too. The problem is that if you call
the bucket's Close() method in your code (and you should call it _somewhere_)
then it will stop working (as expected) for all the other code that currently
has an instance of it. It's just not worth the logistics to bother with a pool
of buckets so create them as one-offs, as needed. (20191213/thisisaaronland)

*/
