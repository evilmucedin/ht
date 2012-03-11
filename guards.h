#pragma once

#include <iostream>

#include "atomic.h"
#include "unordered_map"

#include "transp_holder.h"

namespace NLFHT
{
    class BaseGuard;

    class Guardable
    {
    public:
        virtual BaseGuard* AcquireGuard() = 0;
    };

    class BaseGuardManager;

    class BaseGuard : NonCopyable
    {
    public:
        friend class BaseGuardManager;
        friend class ThreadGuardTable;

    public:
        BaseGuard(BaseGuardManager* parent);
        virtual ~BaseGuard();

        void Release();
        BaseGuard* GetNext()
        {
            return Next;
        }

        void GuardTable(AtomicBase tableNumber)
        {
            m_GuardedTable = tableNumber;
        }
        void StopGuarding()
        {
            m_GuardedTable = NO_TABLE;
        }

        void ForbidPrepareToDelete()
        {
            m_PTDLock = true;
        }
        void AllowPrepareToDelete()
        {
            m_PTDLock = false;
        }

        // JUST TO DEBUG
#ifndef NDEBUG
        inline void OnLocalPut() {
            ++m_LocalPutCnt;
        }
        inline void OnLocalDelete() {
            ++m_LocalDeleteCnt;
        }
        inline void OnLocalLookUp() {
            ++m_LocalLookUpCnt;
        }
        inline void OnLocalCopy() {
            ++m_LocalCopyCnt;
        }
        inline void OnGlobalGet() {
            ++m_GlobalGetCnt;
        }
        inline void OnGlobalPut() {
            ++m_GlobalPutCnt;
        }
#else
        inline void OnLocalPut()
        {
        }
        inline void OnLocalDelete()
        {
        }
        inline void OnLocalLookUp()
        {
        }
        inline void OnLocalCopy()
        {
        }
        inline void OnGlobalGet()
        {
        }
        inline void OnGlobalPut()
        {
        }
#endif

        inline void IncreaseAliveCnt()
        {
            AtomicIncrement(m_AliveCnt);
        }
        inline void DecreaseAliveCnt()
        {
            AtomicDecrement(m_AliveCnt);
        }
        inline void IncreaseKeyCnt()
        {
            AtomicIncrement(m_KeyCnt);
        }

        // JUST TO DEBUG
        virtual std::string ToString();

        size_t GetThreadId() const {
            return m_ThreadId;
        }

    private:
        void Init();

    private:
        static const AtomicBase NO_TABLE;

        BaseGuard* Next;
        BaseGuardManager* m_Parent;

        volatile size_t m_GuardedTable;
        volatile bool m_PTDLock;
        // to exclude probability, that data from different
        // tables are in the same cache line
        char Padding[CACHE_LINE_SIZE];

#ifndef NDEBUG
        // JUST TO DEBUG
        Atomic m_LocalPutCnt, m_LocalCopyCnt, m_LocalDeleteCnt, m_LocalLookUpCnt;
        Atomic m_GlobalPutCnt, m_GlobalGetCnt;
#endif

        Atomic m_AliveCnt;
        Atomic m_KeyCnt;

        volatile size_t m_ThreadId;
    };

    class ThreadGuardTable : NonCopyable
    {
    public:
        static void RegisterTable(Guardable* pTable);
        static void ForgetTable(Guardable* pTable);

        static BaseGuard* ForTable(Guardable* pTable)
        {
            assert(m_GuardTable);
            GuardTable::const_iterator toTable = m_GuardTable->find(pTable);
            assert(toTable != m_GuardTable->end());
            return toTable->second;
        }
    private:
        // yhash_map has too big constant
        // more specialized hash_map should be used here
        typedef std::unordered_map<Guardable*, BaseGuard*> GuardTable;
        static NLFHT_THREAD_LOCAL GuardTable* m_GuardTable;
    };

    class BaseGuardManager
    {
    public:
        friend class BaseGuard;

        BaseGuardManager();

        BaseGuard* GetHead() {
            return m_Head;
        }
        BaseGuard* AcquireGuard();

        size_t GetFirstGuardedTable();

        // returns approximate value
        AtomicBase TotalAliveCnt();

        // returns approximate value
        AtomicBase TotalKeyCnt();
        void ZeroKeyCnt();

        bool CanPrepareToDelete();

        // JUST TO DEBUG
        void PrintStatistics(std::ostream& str);

        std::string ToString();

    private:
        class HeadHolder : public VolatilePointerWrapper<BaseGuard> {
        public:
            HeadHolder(BaseGuardManager* parent)
                : m_Parent(parent)
            {
            }
            inline HeadHolder& operator= (BaseGuard* ptr) {
                Set(ptr);
                return *this;
            }

            ~HeadHolder() {
                BaseGuard* current = Get();
                while (current) {
                    BaseGuard* tmp = current;
                    current = current->Next;
                    delete tmp;
                }
#ifndef NDEBUG
                if (m_Parent->m_GuardsCreated != m_Parent->m_GuardsDeleted) {
                    std::cerr << "GuardsCreated " << m_Parent->m_GuardsCreated << '\n'
                         << "GuardsDeleted " << m_Parent->m_GuardsDeleted << '\n';
                    assert(false && !"Some guard lost");
                }
#endif
            }
        private:
            BaseGuardManager* m_Parent;
        };

        HeadHolder m_Head;

        Atomic m_AliveCnt;
        Atomic m_KeyCnt;

#ifndef NDEBUG
        Atomic m_GuardsCreated;
        Atomic m_GuardsDeleted;
#endif

    private:
        BaseGuard* CreateGuard();
        virtual BaseGuard* NewGuard() {
            return new BaseGuard(this);
        }
    };

    template <class Prt>
    class TGuard : public BaseGuard
    {
    public:
        typedef Prt TParent;
        typedef typename TParent::TGuardManager GuardManager;

        TGuard(GuardManager* parent)
            : BaseGuard(parent)
        {
        }
    };

    template <class Prt>
    class GuardManager : public BaseGuardManager
    {
    public:
        typedef Prt TParent;
        typedef typename TParent::TGuard Guard;

        GuardManager(TParent* parent)
            : m_Parent(parent)
        {
        }

    private:
        TParent* m_Parent;

    private:
        virtual BaseGuard* NewGuard()
        {
            return new Guard(this);
        }
    };
}
