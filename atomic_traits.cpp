#include "atomic_traits.h"

namespace NLFHT
{
    template <>
    std::string AtomicTraits<const char*>::ToString(const AtomicTraits<const char*>::Type& s)
    {
        return s;
    }
}
