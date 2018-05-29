//Public domain
#pragma once

namespace bson {
class NonCopyable
{
  protected:
    NonCopyable () {}
    ~NonCopyable () {} /// Protected non-virtual destructor
  private: 
    NonCopyable (const NonCopyable &);
    NonCopyable & operator = (const NonCopyable &);
};
};
