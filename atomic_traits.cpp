#include "atomic_traits.h"

namespace NLFHT {

    template <>
    std::string TAtomicTraits<const char*>::ToString(const TAtomicTraits<const char*>::TType& s) {
        return s;
    }
}
