#include "lfht.h"

namespace NLFHT
{
    NLFHT_THREAD_LOCAL TGuard* TLFHashTableBase::Guard((TGuard*)0);
};
