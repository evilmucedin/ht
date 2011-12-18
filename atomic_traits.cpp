#include "atomic_traits.h"

namespace NLFHT {

    template <>
    Stroka TAtomicTraits<const char*>::ToString(const TAtomicTraits<const char*>::TType& s) {
        return s;
    }
}
