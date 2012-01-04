#include "atomic.h"

namespace NLFHT {
    template <class K>
    class TKeyTraits;

    template <class V>
    class TValueTraits;

    template <class T, class Alloc = DEFAULT_ALLOCATOR(T)>
    class TBaseManager {
    public:
        typedef Alloc TAllocator;

        void RegisterThread() {
        }
        void ForgetThread() {
        }

        // JUST TO DEBUG
        std::string ToString() {
            return "";
        }
    };

    template <class K, class Alloc = DEFAULT_ALLOCATOR(K)>
    class TDefaultKeyManager : public TBaseManager<Alloc, K> {
    public:
        void UnRef(K, size_t = 1) {
        }
    };

    template <class V, class Alloc = DEFAULT_ALLOCATOR(V)>
    class TDefaultValueManager : public TBaseManager<Alloc, V> {
    public:
        void ReadAndRef(V& dest, typename TValueTraits<V>::TAtomicValue const& src) {
            dest = TValueTraits<V>::PureValue(V(src));
        }

        void UnRef(V, size_t = 1) {
        }
    };
}
