/*
 *  This file is part of Peredvizhnikov Engine
 *  Copyright (C) 2023 Eduard Permyakov 
 *
 *  Peredvizhnikov Engine is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Peredvizhnikov Engine is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

import sync;
import logger;
import event;
import assert;

import <cstdlib>;
import <exception>;
import <vector>;
import <variant>;
import <any>;
import <random>;
import <cmath>;


constexpr int kNumClients = 32;
constexpr int kNumRequests = 500;
constexpr float kEpsilon = 1.0f / 10000;

enum ArithmeticRequestType
{
    eMultiply,
    eDivide,
    eAdd,
    eSubtract,
    eQuit
};

enum ArithmeticResult
{
    eSuccess,
    eDivideByZero
};

struct ArithmeticRequest
{
    double m_first_operand;
    double m_second_operand;
};

class ArithmeticServer : public pe::Task<void, ArithmeticServer>
{
    using Task<void, ArithmeticServer>::Task;

    virtual ArithmeticServer::handle_type Run()
    {
        while(true) {

            auto msg = co_await Receive();
            switch(static_cast<ArithmeticRequestType>(msg.m_header)) {
            case ArithmeticRequestType::eMultiply: {
                auto request = any_cast<ArithmeticRequest>(msg.m_payload);
                Reply(msg.m_sender.lock(), pe::Message{this->shared_from_this(), 
                    ArithmeticResult::eSuccess,
                    request.m_first_operand * request.m_second_operand});
                break;
            }
            case ArithmeticRequestType::eDivide: {
                auto request = any_cast<ArithmeticRequest>(msg.m_payload);
                if(request.m_second_operand < kEpsilon) {
                    Reply(msg.m_sender.lock(), pe::Message{this->shared_from_this(),
                        ArithmeticResult::eDivideByZero, std::nan("0")});
                }else{
                    Reply(msg.m_sender.lock(), pe::Message{this->shared_from_this(),
                        ArithmeticResult::eSuccess,
                        request.m_first_operand / request.m_second_operand});
                }
                break;
            }
            case ArithmeticRequestType::eAdd: {
                auto request = any_cast<ArithmeticRequest>(msg.m_payload);
                Reply(msg.m_sender.lock(), pe::Message{this->shared_from_this(),
                    ArithmeticResult::eSuccess,
                    request.m_first_operand + request.m_second_operand});
                break;
            }
            case ArithmeticRequestType::eSubtract: {
                auto request = any_cast<ArithmeticRequest>(msg.m_payload);
                Reply(msg.m_sender.lock(), pe::Message{this->shared_from_this(),
                    ArithmeticResult::eSuccess,
                    request.m_first_operand - request.m_second_operand});
                break;
            }
            case ArithmeticRequestType::eQuit:
                Reply(msg.m_sender.lock(), pe::Message{this->shared_from_this()});
                co_return;
            }
        }
        co_return;
    }
};

class ArithmeticClient : public pe::Task<void, ArithmeticClient>
{
private:

    using base = Task<void, ArithmeticClient>;
    using base::base;

    pe::weak_ptr<ArithmeticServer> m_server;

    bool check(ArithmeticRequestType type, double a, double b, double answer)
    {
        switch(type) {
        case ArithmeticRequestType::eMultiply:
            return (a * b == answer);
        case ArithmeticRequestType::eDivide:
            if(b < kEpsilon)
                return (answer == std::nan("0"));
            return (a / b == answer);
        case ArithmeticRequestType::eAdd:
            return (a + b == answer);
        case ArithmeticRequestType::eSubtract:
            return (a - b == answer);
        default:
            pe::assert(0);
            return false;
        }
    }

    virtual ArithmeticClient::handle_type Run()
    {
        std::uniform_real_distribution<double> distribution{};
        std::default_random_engine re{};

        for(int i = 0; i < kNumRequests; i++) {
            ArithmeticRequestType types[] = {
                ArithmeticRequestType::eMultiply,
                ArithmeticRequestType::eDivide,
                ArithmeticRequestType::eAdd,
                ArithmeticRequestType::eSubtract
            };
            int op_idx = std::rand() % std::size(types);
            double first = distribution(re);
            double second = distribution(re);

            auto response = co_await Send(m_server.lock(),
                pe::Message{this->shared_from_this(), types[op_idx], 
                ArithmeticRequest{first, second}});
            auto answer = any_cast<double>(response.m_payload);
            pe::assert(check(types[op_idx], first, second, answer), "Unexpected answer!");
        }
        co_return;
    }

public:

    ArithmeticClient(base::TaskCreateToken token, pe::Scheduler& scheduler, 
        pe::Priority priority, pe::CreateMode mode, pe::Affinity affinity, 
        pe::shared_ptr<ArithmeticServer> server)
        : base{token, scheduler, priority, mode, affinity}
        , m_server{server}
    {}
};

class Tester : public pe::Task<void, Tester>
{
    using Task<void, Tester>::Task;

    virtual Tester::handle_type Run()
    {
        pe::ioprint(pe::TextColor::eGreen, "Starting Message stress-testing...");
        auto server = ArithmeticServer::Create(Scheduler());

        std::vector<pe::shared_ptr<ArithmeticClient>> clients{};
        for(int i = 0; i < kNumClients; i++) {
            clients.push_back(ArithmeticClient::Create(Scheduler(),
                pe::Priority::eNormal, pe::CreateMode::eLaunchSync,
                pe::Affinity::eAny, server));
        }

        for(int i = 0; i < kNumClients; i++) {
            co_await clients[i];
        }

        co_await Send(server, pe::Message{this->shared_from_this(), 
            ArithmeticRequestType::eQuit, std::monostate{}});
        co_await server;

        pe::ioprint(pe::TextColor::eGreen, "Testing finished");
        Broadcast<pe::EventType::eQuit>();
    }
};

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::Scheduler scheduler{};
        auto tester = Tester::Create(scheduler);
        scheduler.Run();

    }catch(pe::TaskException &e) {

        e.Print();
        ret = EXIT_FAILURE;

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

