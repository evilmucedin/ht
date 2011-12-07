#include "lfht.h"

namespace NLFHT {
    const AtomicBase Guard::NO_OWNER = 0;
    const AtomicBase Guard::NO_TABLE = static_cast<AtomicBase>(-1);
};
