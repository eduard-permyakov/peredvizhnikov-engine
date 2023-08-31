export module window;

import sync;
import SDL2;

import <exception>;
import <string>;
import <any>;

namespace pe{

using namespace std::string_literals;

class SDLWindow
{
private:

    SDL_Window *m_handle;

public:

    SDLWindow(const std::string& title, std::size_t w, std::size_t h)
        : m_handle{}
    {
        if((m_handle = SDL_CreateWindow(title.c_str(), SDL_WINDOWPOS_CENTERED, 
            SDL_WINDOWPOS_CENTERED, w, h, 0)) == nullptr) {
            throw std::runtime_error{"Failed to create window: "s + SDL_GetError()};
        }
    }

    ~SDLWindow()
    {
        if(m_handle) {
            SDL_DestroyWindow(m_handle);
        }
    }

    void SetTitle(const std::string& title)
    {
        SDL_SetWindowTitle(m_handle, title.c_str());
    }
};

export
class Window : public pe::Task<void, Window, std::string, std::size_t, std::size_t>
{
    using base = Task<void, Window, std::string, std::size_t, std::size_t>;
    using base::base;

    enum WindowOperation
    {
        eSetTitle
    };

    virtual Window::handle_type Run(std::string title, std::size_t w, std::size_t h)
    {
        auto window = SDLWindow(title, w, h);
        while(true) {
            auto msg = co_await Receive();
            switch(static_cast<WindowOperation>(msg.m_header)) {
            case WindowOperation::eSetTitle: {
                auto title = any_cast<std::string>(msg.m_payload);
                window.SetTitle(title);
                break;
            }}
            Reply(msg.m_sender.lock(), {this->shared_from_this()});
        }
    }

public:

    Window(base::TaskCreateToken token, pe::Scheduler& scheduler, 
        pe::Priority priority, pe::CreateMode mode, pe::Affinity affinity)
        : base{token, scheduler, priority, mode, affinity}
    {
        if(affinity != Affinity::eMainThread)
            throw std::invalid_argument{"Task must have main thread affinity."};
    }

    template <pe::CallToken<Window> SendToken>
    auto SetTitle(SendToken token, std::string title)
    {
        return token(
            this->shared_from_this(),
            static_cast<uint64_t>(WindowOperation::eSetTitle),
            title
        );
    }
};

} // namespace pe

