#include <stdio.h>
#include <string.h>
#include <math.h>
#include <algorithm>
#include <vector>
#include "GraphLite.h"
#define VERTEX_CLASS_NAME(name) GraphColor##name

int m_color_number;
int id;

class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex= n;
        return m_total_vertex;
    }
    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge= n;
        return m_total_edge;
    }
    int getVertexValueSize() {
        m_n_value_size = sizeof(int);
        return m_n_value_size;
    }
    int getEdgeValueSize() {
        m_e_value_size = sizeof(int);
        return m_e_value_size;
    }
    int getMessageValueSize() {
        m_m_value_size = sizeof(int);
        return m_m_value_size;
    }
    void loadGraph() {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        double weight = 0;
        
        double value = 1;
        int outdegree = 0;
        
        const char *line= getEdgeLine();
        sscanf(line, "%lld %lld", &from, &to);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++outdegree;
        for (int64_t i = 1; i < m_total_edge; ++i) {
            line= getEdgeLine();
            sscanf(line, "%lld %lld", &from, &to);
            if (last_vertex != from) {
                addVertex(last_vertex, &value, outdegree);
                last_vertex = from;
                outdegree = 1;
            } else {
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        addVertex(last_vertex, &value, outdegree);
    }
};

class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {
public:
    void writeResult() {
        int64_t vid;
        int value;
        char s[1024];

        for (ResultIterator r_iter; ! r_iter.done(); r_iter.next() ) {
            r_iter.getIdValue(vid, &value);
            int n = sprintf(s, "%lld: %d\n", (unsigned long long)vid, value);
            writeNextResLine(s, n);
        }
    }
};
class VERTEX_CLASS_NAME(Aggregator): public Aggregator<int> {
public:
    void init() {
        m_global = 0;
        m_local = 0;
    }
    void* getGlobal() {
        return &m_global;
    }
    void setGlobal(const void* p) {
        m_global = * (int *)p;
    }
    void* getLocal() {
        return &m_local;
    }
    void merge(const void* p) {
        m_global += * (int *)p;
    }
    void accumulate(const void* p) {
        m_local += * (int *)p;
    }
};

class VERTEX_CLASS_NAME(): public Vertex <int, int, int> {
public:
    void compute(MessageIterator* pmsgs) {
       vector<int> neighbor_array;
       int color;
       int max_num=-1;
       if(getSuperstep() == 0){
           if(getVertexId() == id){
               color=0;
           } else{
               color=-1;
           }             
       } else{
            if (getSuperstep() >= 2) {
                int global_val = * (int *)getAggrGlobal(0);
                if (global_val == 0) {
                    voteToHalt(); return;
                }
            }
            for ( ; ! pmsgs->done(); pmsgs->next() ) {
                if(pmsgs->getValue() > max_num){
                    max_num=pmsgs->getValue();
                }
                neighbor_array.push_back(pmsgs->getValue());
            }

            vector <int>::iterator it = find(neighbor_array.begin(), neighbor_array.end(), getValue()); 
            if (it != neighbor_array.end() || getValue() == -1){
                color=max_num+(rand()%(m_color_number-1))+1;
            }else{
                color=getValue();
            }
            int acc = fabs(getValue() - color);
            accumulateAggr(0, &acc);
       }
        * mutableValue() = color;
        sendMessageToAllNeighbors(color);
    }
      
};
class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
    void init(int argc, char* argv[]) {
        
        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 3) {
           printf ("Usage: %s <input path> <output path>\n", argv[0]);
           exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];
       id=atoi(argv[3]);
        m_color_number=atoi(argv[4]);
        
        
        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);
        regAggr(0, &aggregator[0]);
    }

    void term() {
        delete[] aggregator;
    }
};
extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}